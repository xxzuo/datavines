/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datavines.server.api.controller;

import io.datavines.core.constant.DataVinesConstants;
import io.datavines.core.entity.ResultMap;
import io.datavines.server.api.annotation.AuthIgnore;
import io.datavines.server.api.dto.vo.WikiTreeNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Api(value = "wiki", tags = "wiki", produces = MediaType.APPLICATION_JSON_VALUE)
@RestController
@RequestMapping(value = DataVinesConstants.BASE_API_PATH + "/wiki", produces = MediaType.APPLICATION_JSON_VALUE)
public class WikiController {

    private static final Logger logger = LoggerFactory.getLogger(WikiController.class);

    @Value("${datavines.wiki.content-path:docs/wiki}")
    private String wikiContentPath;

    @AuthIgnore
    @ApiOperation(value = "get wiki tree")
    @GetMapping(value = "/tree")
    public Object getWikiTree() {
        File rootDir = new File(wikiContentPath);
        if (!rootDir.exists() || !rootDir.isDirectory()) {
            return new ResultMap().fail().message("Wiki content directory not found");
        }

        List<WikiTreeNode> tree = buildTree(rootDir, "");
        return new ResultMap().success().payload(tree);
    }

    @AuthIgnore
    @ApiOperation(value = "get wiki content")
    @GetMapping(value = "/content")
    public Object getWikiContent(@RequestParam("path") String filePath) {
        if (!isPathSafe(filePath)) {
            return new ResultMap().fail().message("Invalid file path");
        }

        Path fullPath = Paths.get(wikiContentPath, filePath).normalize();
        Path basePath = Paths.get(wikiContentPath).normalize();

        if (!fullPath.startsWith(basePath)) {
            return new ResultMap().fail().message("Invalid file path");
        }

        File file = fullPath.toFile();
        if (!file.exists() || !file.isFile()) {
            return new ResultMap().fail().message("File not found");
        }

        try {
            String content = new String(Files.readAllBytes(fullPath), StandardCharsets.UTF_8);
            return new ResultMap().success().payload(content);
        } catch (IOException e) {
            logger.error("Failed to read wiki file: {}", filePath, e);
            return new ResultMap().fail().message("Failed to read file");
        }
    }

    private boolean isPathSafe(String path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        if (path.contains("..") || path.startsWith("/") || path.startsWith("\\")) {
            return false;
        }
        if (path.contains(":")) {
            return false;
        }
        return true;
    }

    private List<WikiTreeNode> buildTree(File dir, String relativePath) {
        List<WikiTreeNode> nodes = new ArrayList<>();
        File[] files = dir.listFiles();
        if (files == null) {
            return nodes;
        }

        List<File> sortedFiles = new ArrayList<>();
        for (File file : files) {
            sortedFiles.add(file);
        }
        sortedFiles.sort((a, b) -> {
            if (a.isDirectory() && !b.isDirectory()) {
                return -1;
            }
            if (!a.isDirectory() && b.isDirectory()) {
                return 1;
            }
            return a.getName().compareToIgnoreCase(b.getName());
        });

        for (File file : sortedFiles) {
            WikiTreeNode node = new WikiTreeNode();
            node.setName(file.getName());
            String currentPath = relativePath.isEmpty() ? file.getName() : relativePath + "/" + file.getName();
            node.setPath(currentPath);

            if (file.isDirectory()) {
                node.setType("dir");
                node.setChildren(buildTree(file, currentPath));
            } else {
                node.setType("file");
                node.setChildren(null);
            }

            nodes.add(node);
        }

        return nodes;
    }
}
