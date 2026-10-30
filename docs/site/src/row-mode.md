# Row mode and the RowModeReporter hook

Trill runs most operators in **columnar mode**. It stores each batch of events as one array per field and generates code for the operator that works on those arrays. When an operator cannot do that, it falls back to **row mode**, which handles one event object at a time. Row mode gives the same results, and it is usually slower.

Fallback is automatic. Before this hook was added, a fallback left a trace in the operator's `ErrorMessages` only when code generation failed. The hook reports each fallback as it happens, with its reason.

The diagrams on this site run every query in row mode on purpose, through `Config.ForceRowBasedExecution`, because the browser cannot compile the generated code. The hook is not called when that setting is on.

## Setting the hook

`Config.RowModeReporter` is a static `Action<RowModeReport>`. Set it before you build your queries:

```csharp
Config.RowModeReporter = report =>
    Console.WriteLine($"{report.Operator.GetType().Name}: {report.Reason} ({report.Detail})");
```

For example, this payload type has a read-only field:

```csharp
public sealed class Reading { public readonly string Sensor; public int Value; }
```

Building a query with this payload type reports the ingress with the reason `TypeNotColumnar` and the detail `field Sensor is read-only`. Downstream operators generally run in row mode too, without reports of their own, because their input is already in rows.

Some operators directly after the ingress do trigger another report. For an ingress built by calling `ToStreamable()` on `StreamEvent`s, Trill fuses certain operators directly after it into a new ingress. These are `Where`, `Select` and the constant-duration overload `AlterEventDuration(n)`. The new ingress reports the same reason again. The `AlterEventDuration` overloads that take a function are not fused, and add no report.

## What a report contains

| Property | What it holds |
|---|---|
| `Operator` | The operator that falls back: the `IStreamable` instance, or, for egress, the observable the egress method returned. It is typed `object`, so match on its type name. |
| `Reason` | One of the four reasons below. |
| `NonColumnarType` | For `TypeNotColumnar`, the type that failed the check. It may be the operator's input, its output or another type it holds. Otherwise null. |
| `Detail` | A plain-language reason, or null if the operator recorded none. For `TypeNotColumnar`, the rule the type broke. |
| `Diagnostics` | Diagnostics recorded during code generation, or null. They include the compiler's diagnostics if compilation ran. |
| `Exception` | The exception that stopped code generation, or null. |
| `AtSubscribe` | True if the fallback decision was made during a subscription that Trill tracks: through an egress method or `Cache()`. False if it was made while building the query or during an untracked subscription. One untracked path is a direct call to `IStreamable.Subscribe`, which is intended for internal use. |

`report.ToString()` gives a one-line summary, such as `Row mode: <operator> (TypeNotColumnar): field Sensor is read-only`.

## The four reasons

| Reason | When | Look at |
|---|---|---|
| `TypeNotColumnar` | A type the operator would hold in columns cannot be represented as columns. | `NonColumnarType` and `Detail` |
| `CodeGenerationFailed` | Trill tried to generate the operator's columnar code and failed. | `Diagnostics` and `Exception` |
| `PartitionedStream` | The stream is partitioned, and the operator has no columnar form for partitioned streams. | `Detail` is "partitioned streams run in row mode" |
| `NotSupported` | The operator has no columnar form for this query. | `Detail`, when the operator recorded a reason; otherwise null |

### Types that cannot be held in columns

For `TypeNotColumnar`, `Detail` names the first rule the type breaks. Trill stops at the first failure. It follows the table from top to bottom, treating the three consecutive property checks as a group. For each property, it checks for a missing setter, a non-public getter or setter, and an init-only setter, in that order. It then moves to the next property in reflection order.

| `Detail` | Typical cause |
|---|---|
| `field X has an anonymous type` | A public field whose type is anonymous. |
| `property X has an anonymous type` | An anonymous type with a property whose type is also anonymous. An anonymous type with no such property passes the check without applying the remaining rules. |
| `the type is not public` | Generated code must name the type, so it must be visible outside its assembly. |
| `field X is not public` | A non-public instance field other than an auto-property's backing field. An F# record stops here, because it keeps its state in fields such as `Key@`. |
| `field X is read-only` | A `readonly` field, as in a `readonly struct`. |
| `property X has no setter` | A get-only auto-property. |
| `property X is not public` | An auto-property whose getter or setter is not public. |
| `property X has an init-only setter` | A `{ get; init; }` property. A C# positional record stops here. |
| `the type is abstract or an interface` | A class that is abstract, or an interface. |
| `the type has no public parameterless constructor` | A class that Trill cannot create. |

A struct skips the last two rules. For types other than anonymous types, columnar mode requires a public type that keeps its state in public, settable fields and auto-properties. A class must also be concrete and have a public parameterless constructor.

## When the hook is called

- On the thread that builds or subscribes to the query, at the moment the operator decides. It is never called while data flows.
- Once for each fallback decision, with no deduplication. A query built twice reports twice. Map-reduce builds its query in its constructor and again at `Subscribe`, and each build reports. `Multicast` builds its selector once to work out the stream's properties, and that build reports too.
- It is process-wide: it sees reports from every query, and it may be called on several threads at once.
- Not at all when `Config.ForceRowBasedExecution` is set, and not for a code generation failure that throws because fallback to row mode is disabled.
- If the hook throws, the exception is caught and counted in `Config.RowModeReporterFailures`, and the query is built as usual.

Operators that convert between rows and columns on purpose, such as `Union`, do not report. A binary operator also does not report when it converts one input to rows because the other input is already in rows. The operator upstream that fell back has already reported.

**See also:** [Reading the diagrams](introduction.md)
