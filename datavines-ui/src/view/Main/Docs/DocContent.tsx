import React, { useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import { Spin } from 'antd';
import MermaidRenderer from './MermaidRenderer';
import 'highlight.js/styles/github.css';

interface DocContentProps {
    content: string;
    loading: boolean;
    onNavigate?: (path: string) => void;
}

const preprocessContent = (content: string): string => {
    // Strip <cite>...</cite> blocks (source file reference metadata)
    return content.replace(/<cite>[\s\S]*?<\/cite>/gi, '');
};

const DocContent: React.FC<DocContentProps> = ({ content, loading, onNavigate }) => {
    const handleLinkClick = useCallback((e: React.MouseEvent<HTMLAnchorElement>, href: string) => {
        if (!href) return;

        // Anchor links: scroll to heading within the document
        if (href.startsWith('#')) {
            e.preventDefault();
            const id = decodeURIComponent(href.slice(1));
            const container = (e.target as HTMLElement).closest('.dv-docs-content');
            if (container) {
                const target = container.querySelector(`[id="${id}"]`)
                    || container.querySelector(`[id="${id.toLowerCase()}"]`);
                if (target) {
                    target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            }
            return;
        }

        // file:// links: source code references, not navigable in web
        if (href.startsWith('file://')) {
            e.preventDefault();
            return;
        }

        // Relative .md links: navigate within wiki
        if (href.endsWith('.md') || href.endsWith('.MD')) {
            e.preventDefault();
            if (onNavigate) {
                onNavigate(href);
            }
            return;
        }

        // External links: open in new tab
        if (href.startsWith('http://') || href.startsWith('https://')) {
            e.preventDefault();
            window.open(href, '_blank', 'noopener,noreferrer');
            return;
        }
    }, [onNavigate]);

    if (loading) {
        return (
            <div className="dv-docs-content-empty">
                <Spin />
            </div>
        );
    }

    if (!content) {
        return null;
    }

    const processedContent = preprocessContent(content);

    return (
        <div className="dv-docs-markdown">
            <ReactMarkdown
                remarkPlugins={[remarkGfm]}
                rehypePlugins={[[rehypeHighlight, { ignoreMissing: true }]]}
                components={{
                    pre({ node, children, ...props }: any) {
                        const child = Array.isArray(children) ? children[0] : children;
                        if (child?.props?.className && /language-mermaid/.test(child.props.className)) {
                            const code = String(child.props.children || '').trim();
                            return <MermaidRenderer code={code} />;
                        }
                        return <pre {...props}>{children}</pre>;
                    },
                    a({ node, href, children, ...props }: any) {
                        const isFileRef = href?.startsWith('file://');
                        if (isFileRef) {
                            // Render file:// links as plain styled text
                            return <code className="dv-docs-file-ref">{children}</code>;
                        }
                        return (
                            <a
                                href={href}
                                {...props}
                                onClick={(e: React.MouseEvent<HTMLAnchorElement>) => handleLinkClick(e, href || '')}
                            >
                                {children}
                            </a>
                        );
                    },
                    // Generate id for headings to support anchor links
                    h1({ node, children, ...props }: any) {
                        const id = getHeadingId(children);
                        return <h1 id={id} {...props}>{children}</h1>;
                    },
                    h2({ node, children, ...props }: any) {
                        const id = getHeadingId(children);
                        return <h2 id={id} {...props}>{children}</h2>;
                    },
                    h3({ node, children, ...props }: any) {
                        const id = getHeadingId(children);
                        return <h3 id={id} {...props}>{children}</h3>;
                    },
                    h4({ node, children, ...props }: any) {
                        const id = getHeadingId(children);
                        return <h4 id={id} {...props}>{children}</h4>;
                    },
                }}
            >
                {processedContent}
            </ReactMarkdown>
        </div>
    );
};

function getHeadingId(children: any): string {
    const text = extractText(children);
    return text.toLowerCase().replace(/\s+/g, '-').replace(/[^\w\u4e00-\u9fff-]/g, '');
}

function extractText(children: any): string {
    if (typeof children === 'string') return children;
    if (Array.isArray(children)) return children.map(extractText).join('');
    if (children?.props?.children) return extractText(children.props.children);
    return '';
}

export default DocContent;
