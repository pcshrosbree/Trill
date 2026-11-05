// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class WhereStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        public readonly Expression<Func<TPayload, bool>> Predicate;

        public WhereStreamable(IStreamable<TKey, TPayload> source, Expression<Func<TPayload, bool>> predicate)
            : base(source, source.Properties.Where(predicate))
        {
            Contract.Requires(source != null);
            Contract.Requires(predicate != null);

            this.Predicate = predicate;
            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
            => this.Properties.IsColumnar
            ? GetPipe(observer)
            : new WherePipe<TKey, TPayload>(this, observer);

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTSource = typeof(TPayload);

            if (!typeOfTSource.CanRepresentAsColumnar()) return RefuseType(typeOfTSource);
            if (typeOfTKey.GetPartitionType() != null) return RefusePartitioned();

            // A predicate that can write into a struct payload's columns runs on a per-row copy, which the generated
            // code builds with the type's parameterless constructor. If that constructor is not trivial, the Where
            // runs in row mode, where the predicate gets a plain copy.
            var constructor = typeOfTSource.GetConstructor(Type.EmptyTypes);
            if (typeOfTSource.GetTypeInfo().IsValueType && constructor != null && !constructor.IsTrivialParameterlessConstructor()
                && ColumnWriteFinder.Find(this.Predicate, new ColumnarRepresentation(typeOfTSource)))
            {
                return RefuseUnsupported("the predicate can change a struct payload whose parameterless constructor is not trivial");
            }

            var lookupKey = CacheKey.Create(this.Predicate.CacheKeyText());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => WhereTemplate.Generate(this));

            return Generated(generatedPipeType);
        }

        private UnaryPipe<TKey, TPayload, TPayload> GetPipe(IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(this.Predicate.CacheKeyText());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => WhereTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new WherePlanNode(p, o, typeof(TKey), typeof(TPayload), this.Predicate, true, generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (UnaryPipe<TKey, TPayload, TPayload>)instance;
            return returnValue;
        }

        public override string ToString() => "Where(" + this.Predicate.CacheKeyText() + ")";
    }
}