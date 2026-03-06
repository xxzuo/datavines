import React, { useEffect, useRef, useState } from 'react';

let mermaidInstance: any = null;
let mermaidLoadPromise: Promise<any> | null = null;
let idCounter = 0;

const loadMermaid = (): Promise<any> => {
    if (mermaidInstance) {
        return Promise.resolve(mermaidInstance);
    }
    if (!mermaidLoadPromise) {
        mermaidLoadPromise = import('mermaid').then((m) => {
            const mermaid = m.default || m;
            mermaid.initialize({
                startOnLoad: false,
                theme: 'default',
                securityLevel: 'loose',
            });
            mermaidInstance = mermaid;
            return mermaid;
        });
    }
    return mermaidLoadPromise;
};

interface MermaidRendererProps {
    code: string;
}

const MermaidRenderer: React.FC<MermaidRendererProps> = ({ code }) => {
    const [svg, setSvg] = useState<string>('');
    const [error, setError] = useState<string>('');
    const containerRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!code) return;

        let cancelled = false;
        const id = `mermaid-${Date.now()}-${++idCounter}`;

        loadMermaid()
            .then((mermaid) => {
                if (cancelled) return;
                try {
                    mermaid.render(id, code.trim(), (svgCode: string) => {
                        if (!cancelled) {
                            setSvg(svgCode);
                            setError('');
                        }
                    });
                } catch (e: any) {
                    if (!cancelled) {
                        setError(e.message || 'Mermaid render error');
                        setSvg('');
                    }
                    // mermaid.render may leave a broken element in the DOM
                    const errEl = document.getElementById('d' + id);
                    if (errEl) errEl.remove();
                }
            })
            .catch((e) => {
                if (!cancelled) {
                    setError('Failed to load mermaid library');
                }
            });

        return () => {
            cancelled = true;
        };
    }, [code]);

    if (error) {
        return (
            <div className="dv-mermaid-container dv-mermaid-error">
                <pre><code>{code}</code></pre>
            </div>
        );
    }

    if (!svg) {
        return (
            <div className="dv-mermaid-container dv-mermaid-loading">
                Loading diagram...
            </div>
        );
    }

    return (
        <div
            className="dv-mermaid-container"
            ref={containerRef}
            dangerouslySetInnerHTML={{ __html: svg }}
        />
    );
};

export default MermaidRenderer;
