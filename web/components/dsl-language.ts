import { StreamLanguage, type StringStream } from "@codemirror/language";

// A hand-rolled StreamLanguage tokenizer mirroring internal/query/lexer.go's
// token classes closely enough for editor highlighting -- it does not need
// to be a full grammar (CodeMirror's StreamLanguage is intentionally a
// lighter-weight fit for a small DSL like this than a full Lezer parser).
// The token names returned below ("keyword", "string", "number", ...) are
// CodeMirror's standard legacy-mode style names, which StreamLanguage maps
// to highlighting tags automatically -- no separate styleTags/NodeProp
// wiring is needed for a stream (non-Lezer) language.
const KEYWORDS = new Set([
  "trace",
  "since",
  "range",
  "now",
  "by",
  "sort",
  "asc",
  "desc",
  "limit",
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "p50",
  "p95",
  "p99",
]);

const FIELDS = new Set([
  "service",
  "operation",
  "status",
  "kind",
  "duration",
  "trace_id",
  "span_id",
  "parent_span_id",
  "sampling_weight",
]);

interface DSLState {
  inSelector: boolean;
}

function tokenize(stream: StringStream, state: DSLState): string | null {
  if (stream.eatSpace()) return null;

  if (stream.match("{")) {
    state.inSelector = true;
    return "bracket";
  }
  if (stream.match("}")) {
    state.inSelector = false;
    return "bracket";
  }
  if (stream.match(/^[()]/)) return "bracket";
  if (stream.match(",")) return "punctuation";
  if (stream.match("|")) return "operator";

  if (stream.match('"')) {
    while (!stream.eol()) {
      if (stream.match(/^\\./)) continue;
      if (stream.match('"')) break;
      stream.next();
    }
    return "string";
  }

  if (stream.match(/^(=~|!~|==|!=|>=|<=|=|>|<|-)/)) return "operator";

  if (stream.match(/^[0-9]+(\.[0-9]+)?[a-zA-Z]*/)) return "number";

  const ident = stream.match(/^[A-Za-z_][A-Za-z0-9_.]*/);
  if (ident) {
    const word = Array.isArray(ident) ? ident[0] : (ident as unknown as string);
    if (KEYWORDS.has(word)) return "keyword";
    if (FIELDS.has(word)) return "variableName";
    if (state.inSelector) return "propertyName";
    return "atom";
  }

  stream.next();
  return null;
}

export const dslLanguage = StreamLanguage.define<DSLState>({
  startState: () => ({ inSelector: false }),
  token: tokenize,
});
