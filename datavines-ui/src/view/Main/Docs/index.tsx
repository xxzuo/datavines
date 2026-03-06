import React, { useState, useCallback, useEffect } from 'react';
import { useIntl } from 'react-intl';
import { message } from 'antd';
import { $http } from '@/http';
import Title from 'component/Title';
import DocTree, { WikiTreeNode } from './DocTree';
import DocContent from './DocContent';
import './index.less';

const findFirstFile = (nodes: WikiTreeNode[]): string | null => {
    for (const node of nodes) {
        if (node.type === 'file') {
            return node.path;
        }
        if (node.children) {
            const found = findFirstFile(node.children);
            if (found) return found;
        }
    }
    return null;
};

const Docs = () => {
    const intl = useIntl();
    const [treeData, setTreeData] = useState<WikiTreeNode[]>([]);
    const [selectedPath, setSelectedPath] = useState<string>('');
    const [content, setContent] = useState<string>('');
    const [loading, setLoading] = useState<boolean>(false);
    const [treeLoading, setTreeLoading] = useState<boolean>(true);

    const loadTree = async () => {
        try {
            setTreeLoading(true);
            const res = await $http.get('/wiki/tree', {}, { hideError: true });
            const data: WikiTreeNode[] = res || [];
            setTreeData(data);
            const firstFile = findFirstFile(data);
            if (firstFile) {
                setSelectedPath(firstFile);
                loadContent(firstFile);
            }
        } catch (error) {
            console.warn('Failed to load wiki tree:', error);
        } finally {
            setTreeLoading(false);
        }
    };

    const loadContent = async (path: string) => {
        try {
            setLoading(true);
            const res = await $http.get('/wiki/content', { path }, { hideError: true });
            setContent(res || '');
        } catch (error) {
            message.error(intl.formatMessage({ id: 'common_fail' }));
            setContent('');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        loadTree();
    }, []);

    const onSelectFile = useCallback((path: string) => {
        if (path.endsWith('.md') || path.endsWith('.MD')) {
            setSelectedPath(path);
            loadContent(path);
        }
    }, []);

    return (
        <div className="dv-docs">
            <div className="dv-docs-tree">
                <Title>{intl.formatMessage({ id: 'docs_title' })}</Title>
                {treeLoading ? (
                    <div className="dv-docs-tree-loading">
                        <span className="dv-docs-tree-loading-text">Loading...</span>
                    </div>
                ) : treeData.length > 0 ? (
                    <DocTree
                        treeData={treeData}
                        selectedKey={selectedPath}
                        onSelect={onSelectFile}
                    />
                ) : (
                    <div className="dv-docs-tree-empty">
                        {intl.formatMessage({ id: 'docs_no_content' })}
                    </div>
                )}
            </div>
            <div className="dv-docs-content">
                {content || loading ? (
                    <DocContent content={content} loading={loading} onNavigate={onSelectFile} />
                ) : (
                    <div className="dv-docs-content-empty">
                        {intl.formatMessage({ id: 'docs_select_tip' })}
                    </div>
                )}
            </div>
        </div>
    );
};

export default Docs;
