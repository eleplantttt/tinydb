//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cassert>
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/config.h"
#include "common/exception.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_executor)),
      right_child_executor_(std::move(right_executor)),
      joined_(false),
      reorded_(false) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();

  if (plan_->join_type_ == JoinType::INNER) {
    reorded_ =
        plan_->output_schema_->GetColumn(0).GetName() != plan_->GetLeftPlan()->output_schema_->GetColumn(0).GetName();
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &join_expr = plan_->Predicate();
  auto &left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto &right_schema = plan_->GetRightPlan()->OutputSchema();

  while (true) {
    if (!left_tuple_.IsAllocated()) {
      const auto status = left_child_executor_->Next(&left_tuple_, rid);
      if (!status) {
        return false;
      }
    }

    Tuple right_tuple;

    while (right_child_executor_->Next(&right_tuple, rid)) {
      auto value = join_expr.EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
      // fmt::print("left:{} \t right:{} \t joined:{}\n", left_tuple_.ToString(&left_schema),
      //            right_tuple.ToString(&right_schema), ok);
      if (!value.IsNull() && value.GetAs<bool>()) {
        joined_ = true;
        std::vector<Value> vec = GenerateValue(&left_tuple_, left_schema, &right_tuple, right_schema);
        *tuple = Tuple{std::move(vec), &GetOutputSchema()};  // avoid copy
        return true;
      }
    }

    if (plan_->GetJoinType() == JoinType::LEFT && !joined_) {
      std::vector<Value> vec = GenerateValue(&left_tuple_, left_schema, nullptr, right_schema);
      *tuple = Tuple{std::move(vec), &GetOutputSchema()};
      AnotherLoop();
      return true;
    }

    AnotherLoop();
  }
}

void NestedLoopJoinExecutor::AnotherLoop() {
  // 下一轮循环
  left_tuple_ = {};  // not allocated
  joined_ = false;
  right_child_executor_->Init();
}

auto NestedLoopJoinExecutor::GenerateValue(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                                           const Schema &right_schema) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  if (reorded_) {
    assert(right_tuple);
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
      values.push_back(right_tuple->GetValue(&right_schema, i));
    }
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {  // left_tuple.GetLength() xxxxxx
      values.push_back(left_tuple->GetValue(&left_schema, i));
    }
  } else {
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {  // left_tuple.GetLength() xxxxxx
      values.push_back(left_tuple->GetValue(&left_schema, i));
    }
    if (right_tuple != nullptr) {
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.push_back(right_tuple->GetValue(&right_schema, i));
      }
    } else {
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        auto type_id = right_schema.GetColumn(i).GetType();
        values.push_back(ValueFactory::GetNullValueByType(type_id));
      }
    }
  }
  return values;
}

}  // namespace bustub
