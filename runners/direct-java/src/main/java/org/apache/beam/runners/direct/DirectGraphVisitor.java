/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.direct.ViewOverrideFactory.WriteView;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;

/**
 * Tracks the {@link AppliedPTransform AppliedPTransforms} that consume each {@link PValue} in the
 * {@link Pipeline}. This is used to schedule consuming {@link PTransform PTransforms} to consume
 * input after the upstream transform has produced and committed output.
 */
class DirectGraphVisitor extends PipelineVisitor.Defaults {

  private Map<PCollection<?>, AppliedPTransform<?, ?, ?>> producers = new HashMap<>();
  private Map<PCollectionView<?>, AppliedPTransform<?, ?, ?>> viewWriters = new HashMap<>();
  private Set<PCollectionView<?>> consumedViews = new HashSet<>();

  private ListMultimap<PInput, AppliedPTransform<?, ?, ?>> primitiveConsumers =
      ArrayListMultimap.create();

  private Set<AppliedPTransform<?, ?, ?>> rootTransforms = new HashSet<>();
  private Map<AppliedPTransform<?, ?, ?>, String> stepNames = new HashMap<>();
  private int numTransforms = 0;
  private boolean finalized = false;

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    checkState(
        !finalized,
        "Attempting to traverse a pipeline (node %s) with a %s "
            + "which has already visited a Pipeline and is finalized",
        node.getFullName(),
        getClass().getSimpleName());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    checkState(
        !finalized,
        "Attempting to traverse a pipeline (node %s) with a %s which is already finalized",
        node.getFullName(),
        getClass().getSimpleName());
    if (node.isRootNode()) {
      finalized = true;
      checkState(
          viewWriters.keySet().containsAll(consumedViews),
          "All %ss that are consumed must be written by some %s %s: Missing %s",
          PCollectionView.class.getSimpleName(),
          WriteView.class.getSimpleName(),
          PTransform.class.getSimpleName(),
          Sets.difference(consumedViews, viewWriters.keySet()));
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    AppliedPTransform<?, ?, ?> appliedTransform = getAppliedTransform(node);
    stepNames.put(appliedTransform, genStepName());
    if (node.getInputs().isEmpty()) {
      rootTransforms.add(appliedTransform);
    } else {
      for (PValue value : node.getInputs().values()) {
        primitiveConsumers.put(value, appliedTransform);
      }
    }
    if (node.getTransform() instanceof ParDo.MultiOutput) {
      consumedViews.addAll(((ParDo.MultiOutput<?, ?>) node.getTransform()).getSideInputs());
    } else if (node.getTransform() instanceof ViewOverrideFactory.WriteView) {
      viewWriters.put(
          ((WriteView) node.getTransform()).getView(), node.toAppliedPTransform(getPipeline()));
    }
  }

 @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    AppliedPTransform<?, ?, ?> appliedTransform = getAppliedTransform(producer);
    if (value instanceof PCollection && !producers.containsKey(value)) {
      producers.put((PCollection<?>) value, appliedTransform);
    }
  }

  private AppliedPTransform<?, ?, ?> getAppliedTransform(TransformHierarchy.Node node) {
    @SuppressWarnings({"rawtypes", "unchecked"})
    AppliedPTransform<?, ?, ?> application = node.toAppliedPTransform(getPipeline());
    return application;
  }

  private String genStepName() {
    return String.format("s%s", numTransforms++);
  }

  /**
   * Get the graph constructed by this {@link DirectGraphVisitor}, which provides lookups for
   * producers and consumers of {@link PValue PValues}.
   */
  public DirectGraph getGraph() {
    checkState(finalized, "Can't get a graph before the Pipeline has been completely traversed");
    return DirectGraph.create(
        producers, viewWriters, primitiveConsumers, rootTransforms, stepNames);
  }
}
