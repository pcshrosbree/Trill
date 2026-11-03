# Meshner's fork of Trill

This is a fork of [microsoft/Trill](https://github.com/microsoft/Trill). I maintain it for Meshner, a .NET profiler. Microsoft last released Trill in 2021. That release misidentifies .NET 7 and later as .NET Framework. On .NET 10, batch input failed in my tests. Per-event input appeared to run without generated code, with no warning about the fallback.

The fork starts from Microsoft's `master` at commit `b3d6b9bb48`. Each change is kept small and separate, so that it can be offered back to microsoft/Trill as its own pull request.

Done so far:

- The library and its tests target .NET 10 (`net10.0`) only.
- Engine fixes for lost batches in `BinaryPipe`, a `CountAggregate.Sum` that subtracted, wrong `Pivot` results when an interval event ended while its group lived on, `Unpivot` mishandling `int` and `int?` members, `Reduce()` with no arguments throwing after a keyed `Map`, and wrong decisions about columnar mode for some payload types and operators.
- Fixes to code generation. Generated code can reach non-public members of user assemblies. The code generator writes bool, char, string, numeric and enum constants so that they keep their type and value, whatever the current culture.
- Captured variables. Most operators now read a local variable captured by a lambda once, when the query is built. This applies only if the variable holds a bool, char, string, number or enum and the lambda does not assign to it. Inside the reduce function of a map-reduce operation, the value is read at each subscription instead. Reading captured variables this way removes one reason for an operator to fall back to row mode. [Pull request #22](https://github.com/pcshrosbree/Trill/pull/22) has the details.
- Diagnostics. When fallback to row mode is disabled, the exception for a code-generation failure now names the operator and gives the compiler's errors. An application can set `Config.RowModeReporter` to be told about each operator that falls back to row mode.
- Columnar mode. A `GroupAggregate` with a `string` result now falls back to row mode instead of throwing. So does `ConsecutivePairs` when its class payload uses default equality, implements `IEquatable<T>` or overrides `Equals`. On a stream of start edges only, aggregates inside `GroupApply` no longer give later results the group's first start time.
- `Unpivot` uses reference equality when it checks members for null, and boxes value types assigned to a value field whose type is `object` or an interface. It can now run in columnar mode.
- `SessionTimeoutWindow` refuses a timeout of 0 or less. It uses a session's first event to decide where to cut a long session, and with a grouping key it keeps its output in time order. Its XML docs and operator page now say how sessions are cut and which events each execution mode emits.
- Percentiles. `PercentileDiscrete(0)` returns the first value. If `percentile * count` is just above a whole number, `PercentileDiscrete` treats it as that whole number, so `PercentileDiscrete(0.07)` over 100 values gives the 7th value, not the 8th. `PercentileContinuous` and `PercentileDiscrete` can be called directly on a stream, as `Sum` can.
- `TopK`. When payloads tie and their type has no comparison of its own, `TopK` compares their public fields and properties to break most ties, where it used to throw. Its XML docs now say how it orders and ranks results.

The [merged pull requests](https://github.com/pcshrosbree/Trill/pulls?q=is%3Apr+is%3Amerged) list every change.

Still to do:

- more engine fixes, including hash mixing in `FastDictionary`;
- publish the result as the NuGet package `Meshner.Trill`, keeping the namespace `Microsoft.StreamProcessing`.

Some changes come from Amir Burbea's modernisation fork ([amirburbea/Trill](https://github.com/amirburbea/Trill), branch `modernize-net10`), with him as their author.

Issues are welcome here.

## Operator reference

[pcshrosbree.github.io/Trill](https://pcshrosbree.github.io/Trill/) describes Trill's operators. Each operator page has an interactive diagram whose output is computed by Trill itself, running in the browser.

## How this fork is developed

I write changes with AI assistance, using Anthropic's Claude and OpenAI's Codex, and I review every change before it lands. Changes land through pull requests in this repository, once CI passes and the review is complete.

The original README follows.

---

# Introduction

Trill is a high-performance one-pass in-memory streaming analytics engine from Microsoft Research. It can handle both real-time and offline data, and is based on a temporal data and query model. Trill can be used as a streaming engine, a lightweight in-memory relational engine, and as a progressive query processor (for early query results on partial data).

# Getting Started

## Building Trill
1. Of course, the sources are right here!
2. Clone the Repo and make sure you have Visual Studio 2017 installed
3. Open `Trill.sln` solution available in `./Sources` with Visual Studio 2017 
4. Build Trill

## Samples using Trill
If you don't want to compile Trill yourself, you can get binaries from our [NuGet feed](https://www.nuget.org/packages/Trill/).
Samples of Trill usage are available at our [samples repository](https://github.com/Microsoft/TrillSamples). Make sure you start from the Hello World sample to get confident with Trill.

# Learn More
- Announcement [blog post](https://azure.microsoft.com/en-us/blog/microsoft-open-sources-trill-to-deliver-insights-on-a-trillion-events-a-day/).
- The [Trill paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/trill-vldb2015.pdf) appeared at VLDB.
- An [article](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/trill-debull.pdf) on Trill appeared in the IEEE Data Engineering Bulletin.
- The [Trill technical report](http://research.microsoft.com/pubs/214609/trill-TR.pdf).
- Additional documents located in the [/Documentation](./Documentation) directory

# Contact/Feedback
You can create Git issues in this repo, or contact the team using this [email](mailto:asktrill@microsoft.com).

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
