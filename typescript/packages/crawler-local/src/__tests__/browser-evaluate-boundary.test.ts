import { relative, resolve } from "node:path";

import ts from "typescript";
import { describe, expect, it } from "vitest";

interface CaptureFinding {
  readonly callbackLine: number;
  readonly declaration: string;
  readonly file: string;
  readonly identifier: string;
  readonly line: number;
}

interface EvaluateAnalysis {
  readonly callbackCount: number;
  readonly captures: readonly CaptureFinding[];
  readonly nonInlineCallbacks: readonly string[];
}

const TEST_DIRECTORY = import.meta.dirname;
const PACKAGE_DIRECTORY = resolve(TEST_DIRECTORY, "../..");
const SOURCE_DIRECTORY = resolve(PACKAGE_DIRECTORY, "src");
// TypeScript occasionally leaves intrinsic globals symbol-less depending on
// the selected module host. Keep this fallback intentionally tiny.
const UNRESOLVED_BROWSER_GLOBALS: ReadonlySet<string> = new Set([
  "globalThis",
  "undefined",
]);

function createPackageProgram(): ts.Program {
  const configPath = resolve(PACKAGE_DIRECTORY, "tsconfig.json");
  const config = ts.readConfigFile(configPath, (path) => ts.sys.readFile(path));
  if (config.error) throw new Error(formatDiagnostic(config.error));
  const parsed = ts.parseJsonConfigFileContent(
    config.config,
    ts.sys,
    PACKAGE_DIRECTORY,
    undefined,
    configPath,
  );
  if (parsed.errors.length > 0) {
    throw new Error(parsed.errors.map(formatDiagnostic).join("\n"));
  }
  return ts.createProgram({
    options: parsed.options,
    rootNames: parsed.fileNames,
  });
}

function createFixtureProgram(
  source: string,
  dependencies: Readonly<Record<string, string>> = {},
): ts.Program {
  const files = new Map<string, string>([["/fixture.ts", source]]);
  for (const [path, contents] of Object.entries(dependencies)) {
    files.set(resolve("/", path), contents);
  }
  const options: ts.CompilerOptions = {
    lib: ["lib.es2024.d.ts", "lib.dom.d.ts", "lib.dom.iterable.d.ts"],
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    noEmit: true,
    strict: true,
    target: ts.ScriptTarget.ES2024,
    types: [],
  };
  const host = ts.createCompilerHost(options, true);
  const defaultFileExists = host.fileExists.bind(host);
  const defaultGetSourceFile = host.getSourceFile.bind(host);
  const defaultReadFile = host.readFile.bind(host);
  host.fileExists = (path) => files.has(path) || defaultFileExists(path);
  host.readFile = (path) => files.get(path) ?? defaultReadFile(path);
  host.getSourceFile = (path, languageVersion, onError, shouldCreate) => {
    const contents = files.get(path);
    return contents === undefined
      ? defaultGetSourceFile(path, languageVersion, onError, shouldCreate)
      : ts.createSourceFile(path, contents, languageVersion, true);
  };
  const rootNames: string[] = [];
  const paths = files.keys();
  let nextPath = paths.next();
  while (!nextPath.done) {
    rootNames.push(nextPath.value);
    nextPath = paths.next();
  }
  return ts.createProgram({
    host,
    options,
    rootNames,
  });
}

function analyzeEvaluateCallbacks(
  program: ts.Program,
  sourceFiles: readonly ts.SourceFile[],
): EvaluateAnalysis {
  const checker = program.getTypeChecker();
  const captures: CaptureFinding[] = [];
  const nonInlineCallbacks: string[] = [];
  let callbackCount = 0;
  for (const sourceFile of sourceFiles) {
    const visit = (node: ts.Node): void => {
      if (ts.isCallExpression(node) && isEvaluateCall(node)) {
        const callback = node.arguments[0];
        if (
          !callback ||
          (!ts.isArrowFunction(callback) && !ts.isFunctionExpression(callback))
        ) {
          nonInlineCallbacks.push(formatLocation(sourceFile, node));
        } else {
          callbackCount += 1;
          inspectCallback(program, checker, sourceFile, callback, captures);
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(sourceFile);
  }
  return { callbackCount, captures, nonInlineCallbacks };
}

function inspectCallback(
  program: ts.Program,
  checker: ts.TypeChecker,
  sourceFile: ts.SourceFile,
  callback: ts.ArrowFunction | ts.FunctionExpression,
  captures: CaptureFinding[],
): void {
  const callbackLine = sourceFile.getLineAndCharacterOfPosition(
    callback.getStart(sourceFile),
  ).line;
  const visit = (node: ts.Node): void => {
    if (
      node.kind === ts.SyntaxKind.ThisKeyword ||
      node.kind === ts.SyntaxKind.SuperKeyword
    ) {
      captures.push({
        callbackLine: callbackLine + 1,
        declaration: "lexical keyword outside browser closure",
        file: relative(PACKAGE_DIRECTORY, sourceFile.fileName),
        identifier: node.kind === ts.SyntaxKind.ThisKeyword ? "this" : "super",
        line:
          sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile))
            .line + 1,
      });
    }
    if (ts.isIdentifier(node) && isRuntimeReference(node, callback)) {
      const symbol = checker.getSymbolAtLocation(node);
      const declarations = symbol?.declarations ?? [];
      const declaredInside = declarations.some((declaration) =>
        isInside(declaration, callback),
      );
      const declaredByStandardLibrary = declarations.some((declaration) =>
        program.isSourceFileDefaultLibrary(declaration.getSourceFile()),
      );
      const declaredByProject = declarations.some((declaration) => {
        const declarationSource = declaration.getSourceFile();
        return (
          !isInside(declaration, callback) &&
          !program.isSourceFileDefaultLibrary(declarationSource) &&
          !program.isSourceFileFromExternalLibrary(declarationSource)
        );
      });
      if (
        declaredInside ||
        (declaredByStandardLibrary && !declaredByProject) ||
        (declarations.length === 0 && UNRESOLVED_BROWSER_GLOBALS.has(node.text))
      ) {
        ts.forEachChild(node, visit);
        return;
      }
      const declaration = declarations.find(
        (candidate) =>
          !program.isSourceFileDefaultLibrary(candidate.getSourceFile()),
      );
      captures.push({
        callbackLine: callbackLine + 1,
        declaration: declaration
          ? formatLocation(declaration.getSourceFile(), declaration)
          : "unresolved",
        file: relative(PACKAGE_DIRECTORY, sourceFile.fileName),
        identifier: node.text,
        line:
          sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile))
            .line + 1,
      });
    }
    ts.forEachChild(node, visit);
  };
  visit(callback);
}

function isEvaluateCall(node: ts.CallExpression): boolean {
  const expression = node.expression;
  return (
    (ts.isPropertyAccessExpression(expression) &&
      expression.name.text === "evaluate") ||
    (ts.isElementAccessExpression(expression) &&
      ts.isStringLiteralLike(expression.argumentExpression) &&
      expression.argumentExpression.text === "evaluate")
  );
}

function isRuntimeReference(
  identifier: ts.Identifier,
  callback: ts.ArrowFunction | ts.FunctionExpression,
): boolean {
  for (
    let current: ts.Node = identifier.parent;
    current !== callback;
    current = current.parent
  ) {
    if (ts.isTypeNode(current)) return false;
  }
  const parent = identifier.parent;
  if (
    (ts.isPropertyAccessExpression(parent) && parent.name === identifier) ||
    (ts.isPropertyAssignment(parent) && parent.name === identifier) ||
    (ts.isMethodDeclaration(parent) && parent.name === identifier) ||
    (ts.isGetAccessorDeclaration(parent) && parent.name === identifier) ||
    (ts.isSetAccessorDeclaration(parent) && parent.name === identifier) ||
    (ts.isBindingElement(parent) && parent.propertyName === identifier) ||
    (ts.isLabeledStatement(parent) && parent.label === identifier) ||
    ((ts.isBreakStatement(parent) || ts.isContinueStatement(parent)) &&
      parent.label === identifier) ||
    ts.isImportSpecifier(parent) ||
    ts.isImportClause(parent) ||
    ts.isNamespaceImport(parent) ||
    ts.isExportSpecifier(parent)
  ) {
    return false;
  }
  return !isDeclarationIdentifier(identifier);
}

function isDeclarationIdentifier(identifier: ts.Identifier): boolean {
  const parent = identifier.parent;
  return (
    ((ts.isVariableDeclaration(parent) ||
      ts.isParameter(parent) ||
      ts.isFunctionDeclaration(parent) ||
      ts.isFunctionExpression(parent) ||
      ts.isClassDeclaration(parent) ||
      ts.isClassExpression(parent) ||
      ts.isInterfaceDeclaration(parent) ||
      ts.isTypeAliasDeclaration(parent) ||
      ts.isEnumDeclaration(parent) ||
      ts.isEnumMember(parent) ||
      ts.isTypeParameterDeclaration(parent)) &&
      parent.name === identifier) ||
    (ts.isBindingElement(parent) && parent.name === identifier)
  );
}

function isInside(node: ts.Node, container: ts.Node): boolean {
  return (
    node.getSourceFile() === container.getSourceFile() &&
    node.pos >= container.pos &&
    node.end <= container.end
  );
}

function formatLocation(sourceFile: ts.SourceFile, node: ts.Node): string {
  const location = sourceFile.getLineAndCharacterOfPosition(
    node.getStart(sourceFile),
  );
  return `${relative(PACKAGE_DIRECTORY, sourceFile.fileName)}:${String(location.line + 1)}`;
}

function formatDiagnostic(diagnostic: ts.Diagnostic): string {
  return ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n");
}

function fixtureCaptures(
  source: string,
  dependencies?: Readonly<Record<string, string>>,
): readonly string[] {
  const program = createFixtureProgram(source, dependencies);
  const fixture = program.getSourceFile("/fixture.ts");
  if (!fixture) throw new Error("Synthetic fixture source is missing");
  return analyzeEvaluateCallbacks(program, [fixture]).captures.map(
    ({ identifier }) => identifier,
  );
}

const PAGE_DECLARATION =
  "declare const page: { evaluate<T, A>(callback: (arg: A) => T, arg?: A): T };";

describe("Playwright evaluation serialization boundary", () => {
  it("keeps every production crawler callback free of project captures", () => {
    const program = createPackageProgram();
    const sourceFiles = program
      .getSourceFiles()
      .filter(
        (sourceFile) =>
          !sourceFile.isDeclarationFile &&
          sourceFile.fileName.startsWith(`${SOURCE_DIRECTORY}/`) &&
          !sourceFile.fileName.includes("/__tests__/"),
      );
    const analysis = analyzeEvaluateCallbacks(program, sourceFiles);
    expect(analysis.nonInlineCallbacks).toEqual([]);
    expect(analysis.callbackCount).toBeGreaterThanOrEqual(7);
    expect(analysis.callbackCount).toBe(7);
    expect(analysis.captures).toEqual([]);
  });

  it.each([
    [
      "external constant",
      "const OUTER = 1; page.evaluate(() => OUTER);",
      "OUTER",
    ],
    [
      "module helper",
      "function helper() { return 1; } page.evaluate(() => helper());",
      "helper",
    ],
    [
      "enclosing local",
      "function run() { const outer = 1; return page.evaluate(() => outer); }",
      "outer",
    ],
    [
      "parameter default",
      "const outer = 1; page.evaluate(({ value = outer }) => value, {});",
      "outer",
    ],
    [
      "nested closure",
      "const outer = 1; page.evaluate(() => () => outer);",
      "outer",
    ],
    [
      "computed key",
      "const key = 'x'; page.evaluate(() => ({ [key]: 1 }));",
      "key",
    ],
    [
      "runtime typeof",
      "const outer = 1; page.evaluate(() => typeof outer);",
      "outer",
    ],
    [
      "lexical this",
      `class Runner { value = 1; run() { return page.evaluate(() => this.value); } }`,
      "this",
    ],
    [
      "lexical super",
      `class Base { value = 1; } class Runner extends Base {
       run() { return page.evaluate(() => super.value); } }`,
      "super",
    ],
    [
      "arrow arguments",
      `function run() { return page.evaluate(() => arguments.length); }`,
      "arguments",
    ],
    [
      "const enum",
      "const enum State { Ready = 1 } page.evaluate(() => State.Ready);",
      "State",
    ],
  ])("rejects %s captures", (_name, body, expected) => {
    expect(fixtureCaptures(`${PAGE_DECLARATION}\n${body}`)).toContain(expected);
  });

  it("rejects imported aliases", () => {
    expect(
      fixtureCaptures(
        `${PAGE_DECLARATION}\nimport { helper as imported } from "./dependency";
         page.evaluate(() => imported());`,
        { "dependency.ts": "export const helper = () => 1;" },
      ),
    ).toContain("imported");
  });

  it.each([
    [
      "passed arguments",
      `const OUTER = 1; page.evaluate(({ value }) => value, { value: OUTER });`,
    ],
    [
      "local bindings",
      `page.evaluate((input) => { const helper = (value: number) => value + 1;
       const local = helper(input); return { local, property: local }; }, 1);`,
    ],
    [
      "browser and ECMAScript globals",
      `page.evaluate(() => { const values = new Map<string, Uint8Array>();
       const bytes = new TextEncoder().encode(document.body.textContent ?? location.href);
       void fetch(new URL(location.href), { signal: AbortSignal.timeout(1) });
       if (!globalThis) throw new Error("missing"); values.set("x", bytes); return values; });`,
    ],
    [
      "erased types and satisfies",
      `type External = { value: number }; page.evaluate((input: External) => {
       const element = document.body satisfies HTMLElement; return input.value + element.children.length;
       }, { value: 1 });`,
    ],
    [
      "type query",
      `const external = 1; type External = typeof external;
       page.evaluate((input: External) => input, 1);`,
    ],
  ])("allows %s", (_name, body) => {
    expect(fixtureCaptures(`${PAGE_DECLARATION}\n${body}`)).toEqual([]);
  });
});
