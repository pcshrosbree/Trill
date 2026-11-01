// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class GroupedWindowStreamable<TKey, TInput, TState, TOutput, TResult> :
        UnaryStreamable<Empty, TInput, TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private readonly StreamProperties<Empty, TInput> sourceProps;
        internal Expression<Func<TInput, TKey>> KeySelector;
        internal Expression<Func<TKey, TOutput, TResult>> ResultSelector;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        internal readonly IAggregate<TInput, TState, TOutput> Aggregate;

        public GroupedWindowStreamable(IStreamable<Empty, TInput> source, IAggregate<TInput, TState, TOutput> aggregate, Expression<Func<TInput, TKey>> keySelector, Expression<Func<TKey, TOutput, TResult>> resultSelector)
            : base(source, source.Properties
                .Group(keySelector = keySelector.InlineCapturedValues())
                .Snapshot(aggregate = aggregate.InlineCapturedValues())
                .Ungroup(resultSelector = resultSelector.InlineCapturedValues()))
        {
            Contract.Requires(source != null);

            this.Aggregate = aggregate;
            this.sourceProps = source.Properties;
            this.KeySelector = keySelector;
            this.ResultSelector = resultSelector;
            if (!this.sourceProps.IsStartEdgeOnly) throw new InvalidOperationException("Cannot use this streamable if the input stream is not guaranteed to be start-edge only.");

            Initialize();
        }

        internal override IStreamObserver<Empty, TInput> CreatePipe(IStreamObserver<Empty, TResult> observer)
            => this.Properties.IsColumnar ? GetPipe(observer) : new GroupedWindowPipe<TKey, TInput, TState, TOutput, TResult>(this, observer);

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTInput = typeof(TInput);

            if (!typeOfTInput.CanRepresentAsColumnar()) return RefuseType(typeOfTInput);
            if (typeOfTKey.GetPartitionType() != null) return RefusePartitioned();

            var generatedPipeType = GeneratedPipeType();

            return Generated(generatedPipeType);
        }

        // The generated pipe can contain the aggregate's expressions as code, so two aggregates that differ only in
        // a constant need different pipes.
        private Tuple<Type, string> GeneratedPipeType()
        {
            var a = this.Aggregate;
            var lookupKey = CacheKey.Create(
                this.KeySelector.ExpressionToCSharp(),
                this.ResultSelector.ExpressionToCSharp(),
                string.Join(
                    "\n",
                    a.InitialState().CacheKeyText(),
                    a.Accumulate().CacheKeyText(),
                    a.Deaccumulate().CacheKeyText(),
                    a.Difference().CacheKeyText(),
                    a.ComputeResult().CacheKeyText()));
            return cachedPipes.GetOrAdd(lookupKey, key => GroupedWindowTemplate.Generate(this));
        }

        private IStreamObserver<Empty, TInput> GetPipe(IStreamObserver<Empty, TResult> observer)
        {
            var generatedPipeType = GeneratedPipeType();
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new GroupedWindowPlanNode<TInput, TState, TOutput>(
                p, o, typeof(TKey), typeof(TInput), typeof(TOutput), this.Aggregate, this.KeySelector, this.ResultSelector,
                true, generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.Aggregate, planNode);
            var returnValue = (IStreamObserver<Empty, TInput>)instance;
            return returnValue;
        }
    }
}