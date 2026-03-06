import React, { useCallback, useMemo, useState } from 'react';
import { Tree, Input } from 'antd';
import { FileTextOutlined, FolderOutlined, FolderOpenOutlined, SearchOutlined } from '@ant-design/icons';

export interface WikiTreeNode {
    name: string;
    path: string;
    type: 'file' | 'dir';
    children?: WikiTreeNode[];
}

interface DocTreeProps {
    treeData: WikiTreeNode[];
    selectedKey: string;
    onSelect: (path: string) => void;
}

const filterTree = (nodes: WikiTreeNode[], keyword: string): WikiTreeNode[] => {
    const lowerKeyword = keyword.toLowerCase();
    return nodes.reduce<WikiTreeNode[]>((acc, node) => {
        const nameMatch = node.name.replace(/\.md$/i, '').toLowerCase().includes(lowerKeyword);
        if (node.type === 'dir' && node.children) {
            const filteredChildren = filterTree(node.children, keyword);
            if (filteredChildren.length > 0 || nameMatch) {
                acc.push({
                    ...node,
                    children: filteredChildren.length > 0 ? filteredChildren : node.children,
                });
            }
        } else if (nameMatch) {
            acc.push(node);
        }
        return acc;
    }, []);
};

const convertToAntTreeData = (nodes: WikiTreeNode[]): any[] => {
    return nodes.map((node) => ({
        key: node.path,
        title: node.name.replace(/\.md$/i, ''),
        isLeaf: node.type === 'file',
        icon: node.type === 'dir' ? (props: any) => (
            props.expanded ? <FolderOpenOutlined /> : <FolderOutlined />
        ) : <FileTextOutlined />,
        children: node.children ? convertToAntTreeData(node.children) : undefined,
    }));
};

const DocTree: React.FC<DocTreeProps> = ({ treeData, selectedKey, onSelect }) => {
    const [searchValue, setSearchValue] = useState('');

    const handleSelect = useCallback((selectedKeys: React.Key[]) => {
        if (selectedKeys.length > 0) {
            onSelect(selectedKeys[0] as string);
        }
    }, [onSelect]);

    const filteredData = useMemo(() => {
        if (!searchValue.trim()) return treeData;
        return filterTree(treeData, searchValue.trim());
    }, [treeData, searchValue]);

    // Only expand first-level directories by default
    const defaultExpandedKeys = useMemo(() => {
        return treeData
            .filter((node) => node.type === 'dir')
            .map((node) => node.path);
    }, [treeData]);

    const isSearching = searchValue.trim().length > 0;

    return (
        <div className="dv-docs-tree-inner">
            <Input
                className="dv-docs-tree-search"
                placeholder="Search..."
                prefix={<SearchOutlined style={{ color: '#bfbfbf' }} />}
                allowClear
                size="small"
                value={searchValue}
                onChange={(e) => setSearchValue(e.target.value)}
            />
            <div className="dv-docs-tree-list">
                {isSearching ? (
                    <Tree
                        showIcon
                        defaultExpandAll
                        key="search"
                        selectedKeys={selectedKey ? [selectedKey] : []}
                        onSelect={handleSelect}
                        treeData={convertToAntTreeData(filteredData)}
                    />
                ) : (
                    <Tree
                        showIcon
                        defaultExpandedKeys={defaultExpandedKeys}
                        key="browse"
                        selectedKeys={selectedKey ? [selectedKey] : []}
                        onSelect={handleSelect}
                        treeData={convertToAntTreeData(filteredData)}
                    />
                )}
            </div>
        </div>
    );
};

export default DocTree;
