import {basicSetup, EditorView} from 'codemirror';
import {EditorState} from '@codemirror/state';
import {keymap, MatchDecorator, ViewPlugin, Decoration} from '@codemirror/view';
import {indentWithTab} from '@codemirror/commands';
import {HighlightStyle, syntaxHighlighting, indentUnit, ensureSyntaxTree, syntaxTree} from '@codemirror/language';
import {linter} from '@codemirror/lint';
import {yaml} from '@codemirror/lang-yaml';
import {tags} from '@lezer/highlight';

const highlighting = HighlightStyle.define([
  {tag: tags.propertyName, class: 'yaml-key'},
  {tag: tags.string, class: 'yaml-string'},
  {tag: tags.number, class: 'yaml-number'},
  {tag: [tags.bool, tags.null], class: 'yaml-literal'},
  {tag: tags.comment, class: 'yaml-comment'},
  {tag: [tags.meta, tags.punctuation], class: 'yaml-punctuation'}
]);

const variables = new MatchDecorator({
  regexp: /\$[A-Za-z_][A-Za-z0-9_]*/g,
  decoration: Decoration.mark({class: 'yaml-variable'})
});

const variableHighlights = ViewPlugin.fromClass(class {
  constructor(view) { this.decorations = variables.createDeco(view); }
  update(update) { this.decorations = variables.updateDeco(update, this.decorations); }
}, {decorations: plugin => plugin.decorations});

// YAML's parser deliberately treats unquoted scalars as Literals. Color their
// common scalar types without coercing values or changing the YAML document.
function scalarDecorations(view) {
  const marks = [];
  const tree = syntaxTree(view.state);
  for (const {from, to} of view.visibleRanges) tree.iterate({from, to, enter(node) {
    if (node.name !== 'Literal' || node.node.parent?.name === 'Key') return;
    const text = view.state.sliceDoc(node.from, node.to);
    const kind = /^(?:true|false|null|~)$/i.test(text) ? 'literal'
      : /^[+-]?(?:0[xob][0-9a-f_]+|(?:[0-9][0-9_]*(?:\.[0-9_]*)?|\.[0-9_]+)(?:e[+-]?[0-9]+)?|\.inf|\.nan)$/i.test(text) ? 'number'
      : 'string';
    marks.push(Decoration.mark({class: 'yaml-' + kind}).range(node.from, node.to));
  }});
  return Decoration.set(marks, true);
}
const scalarHighlights = ViewPlugin.fromClass(class {
  constructor(view) { this.decorations = scalarDecorations(view); }
  update(update) {
    if (update.docChanged || update.viewportChanged || syntaxTree(update.startState) !== syntaxTree(update.state))
      this.decorations = scalarDecorations(update.view);
  }
}, {decorations: plugin => plugin.decorations});

function syntaxDiagnostics(view) {
  const tree = ensureSyntaxTree(view.state, view.state.doc.length, 100);
  if (!tree) return [];
  const diagnostics = [];
  tree.iterate({enter(node) {
    if (node.type.isError) diagnostics.push({
      from: node.from, to: Math.min(view.state.doc.length, Math.max(node.from + 1, node.to)),
      severity: 'error', message: 'Invalid YAML syntax.'
    });
  }});
  return diagnostics;
}

/** All three YAML surfaces use this adapter; programmatic changes are not edits. */
function create(host, {text = '', label = 'YAML', readOnly = false, onChange = () => {}} = {}) {
  let replacing = false;
  const extensions = [
    basicSetup, yaml(), indentUnit.of('  '), EditorState.tabSize.of(2),
    syntaxHighlighting(highlighting), scalarHighlights, variableHighlights,
    EditorView.contentAttributes.of({'aria-label': label, 'aria-readonly': String(readOnly), tabindex: '0'}),
    EditorState.readOnly.of(readOnly), EditorView.editable.of(!readOnly),
    EditorView.updateListener.of(update => {
      if (update.docChanged && !replacing) onChange(update.state.sliceDoc());
    })
  ];
  if (!readOnly) extensions.push(keymap.of([indentWithTab]), linter(syntaxDiagnostics, {delay: 400}));
  const state = value => EditorState.create({
    doc: value,
    extensions: [...extensions, EditorState.lineSeparator.of(value.match(/\r\n|\r|\n/)?.[0] || '\n')]
  });
  const view = new EditorView({state: state(text), parent: host});
  return Object.freeze({
    getText: () => view.state.sliceDoc(),
    setText(value) {
      if (value === view.state.sliceDoc()) return;
      replacing = true;
      try { view.setState(state(value)); } finally { replacing = false; }
    },
    refresh: () => view.requestMeasure(),
    focus: () => view.focus()
  });
}

window.YamlEditors = Object.freeze({create});
