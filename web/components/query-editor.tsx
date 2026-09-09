"use client";

import { autocompletion, type CompletionContext, type CompletionResult } from "@codemirror/autocomplete";
import { defaultKeymap, historyKeymap, history } from "@codemirror/commands";
import { EditorState } from "@codemirror/state";
import { EditorView, keymap, placeholder as cmPlaceholder } from "@codemirror/view";
import { useEffect, useRef } from "react";
import { dslLanguage } from "./dsl-language";

const KEYWORD_COMPLETIONS = [
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "p50",
  "p95",
  "p99",
  "by",
  "sort",
  "limit",
  "since",
  "range",
  "now",
  "asc",
  "desc",
  "duration",
  "status",
  "operation",
  "service",
  "kind",
];

interface QueryEditorProps {
  value: string;
  onChange: (v: string) => void;
  onSubmit: () => void;
  services: string[];
  operations: string[];
}

export function QueryEditor({ value, onChange, onSubmit, services, operations }: QueryEditorProps) {
  const hostRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  // Kept in refs so the completion source (captured once at editor creation)
  // always sees the latest lists without recreating the whole editor.
  const servicesRef = useRef(services);
  const operationsRef = useRef(operations);
  const onSubmitRef = useRef(onSubmit);
  servicesRef.current = services;
  operationsRef.current = operations;
  onSubmitRef.current = onSubmit;

  const completionSource = (context: CompletionContext): CompletionResult | null => {
    const word = context.matchBefore(/[\w.]*/);
    if (!word) return null;
    if (word.from === word.to && !context.explicit) return null;

    const textBefore = context.state.sliceDoc(0, word.from);
    // Value position: right after service=" or operation=" -- suggest known
    // names instead of the generic keyword list.
    const serviceValue = /service\s*(=|=~|!=|!~)\s*"$/.test(textBefore);
    const operationValue = /operation\s*(=|=~|!=|!~)\s*"$/.test(textBefore);

    if (serviceValue) {
      return { from: word.from, options: servicesRef.current.map((s) => ({ label: s, type: "text" })) };
    }
    if (operationValue) {
      return { from: word.from, options: operationsRef.current.map((s) => ({ label: s, type: "text" })) };
    }
    return {
      from: word.from,
      options: [
        ...KEYWORD_COMPLETIONS.map((k) => ({ label: k, type: "keyword" })),
        ...servicesRef.current.map((s) => ({ label: s, type: "text", detail: "service" })),
        ...operationsRef.current.map((s) => ({ label: s, type: "text", detail: "operation" })),
      ],
    };
  };

  useEffect(() => {
    if (!hostRef.current) return;

    const view = new EditorView({
      state: EditorState.create({
        doc: value,
        extensions: [
          history(),
          keymap.of([
            ...defaultKeymap,
            ...historyKeymap,
            {
              key: "Mod-Enter",
              run: () => {
                onSubmitRef.current();
                return true;
              },
            },
          ]),
          dslLanguage,
          autocompletion({ override: [completionSource] }),
          cmPlaceholder('{service="checkout", status=error} | duration > 100ms | count by (operation)'),
          EditorView.updateListener.of((update) => {
            if (update.docChanged) onChange(update.state.doc.toString());
          }),
          EditorView.theme({
            "&": { fontSize: "13px" },
            ".cm-content": { fontFamily: "ui-monospace, monospace", padding: "10px 12px" },
            ".cm-scroller": { minHeight: "80px" },
          }),
        ],
      }),
      parent: hostRef.current,
    });
    viewRef.current = view;
    return () => view.destroy();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- editor is created once; value/onSubmit read via refs/closures intentionally
  }, []);

  // Keep the editor in sync if `value` changes from outside (e.g. an
  // example-query button), without fighting the user's own typing.
  useEffect(() => {
    const view = viewRef.current;
    if (view && view.state.doc.toString() !== value) {
      view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: value } });
    }
  }, [value]);

  return <div ref={hostRef} />;
}
