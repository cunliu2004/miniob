#include "sql/operator/aggregate_physical_operator.h"
RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }


  return RC::SUCCESS;
}
RC AggregatePhysicalOperator::next(){
  if (result_tuple_.cell_num()>0){
    return RC::RECORD_EOF;
  }
  RC rc=RC::SUCCESS;
  PhysicalOperator *oper=children_[0].get();
  int number=(int)aggregations_.size();
  std::vector<Value> result_cells(number);
  int count=0;
  std::vector<std::vector<Value>> AggrValues(number);
  while(RC::SUCCESS==(rc=oper->next())){
    Tuple*tuple=oper->current_tuple();
    for(int cell_idx=0;cell_idx<(int)aggregations_.size();cell_idx++){
        const AggrOp aggregation=aggregations_[cell_idx];
        Value cell;
        tuple->cell_at(cell_idx, cell);
        AggrValues[cell_idx].emplace_back(cell);
    }
  }
  for(int i=0;i<aggregations_.size();i++){
    if (AggrValues[i].empty()) return RC::EMPTY;
      switch(aggregations_[i]){
        case AggrOp::AGGR_SUM:{
          AttrType type = AggrValues[i][0].attr_type();
          if(type==AttrType::FLOATS||type==AttrType::INTS){
            double Ans = 0;
            for (Value& Val: AggrValues[i]) Ans += Val.get_float();
            result_cells[i].set_type(FLOATS);
            result_cells[i].set_float(Ans);
          }
          else
            return RC::UNIMPLENMENT;
        }
        break;
        case AggrOp::AGGR_MAX:{
          AttrType type=AggrValues[i][0].attr_type();
          std::vector<Value>& index_column_values = AggrValues[i]; 
          Value max=index_column_values[0];
          for(int j=0;j<index_column_values.size();j++){
              Value current_value=index_column_values[j];
              if(current_value.compare(max)>0){
                max=current_value;
              }
          }
          result_cells[i].set_type(type);
          result_cells[i].set_value(max);
        }
        break;
        case AggrOp::AGGR_MIN:{
          AttrType type=AggrValues[i][0].attr_type();
          std::vector<Value>& index_column_values = AggrValues[i]; 
          Value min=index_column_values[0];
          for(int j=0;j<index_column_values.size();j++){
              Value current_value=index_column_values[j];
              if(current_value.compare(min)<0){
                min=current_value;
              }
          }
          result_cells[i].set_type(type);
          result_cells[i].set_value(min);
        }
        break;
        case AggrOp::AGGR_AVG:{
          AttrType type = AggrValues[i][0].attr_type();
          if(type==AttrType::FLOATS||type==AttrType::INTS){
            double Ans = 0;
            for (Value& Val: AggrValues[i]) Ans += Val.get_float();
            std::vector<Value> index_column_values = AggrValues[i]; 
            Ans=Ans/index_column_values.size();
            result_cells[i].set_type(FLOATS);
            result_cells[i].set_float(Ans);
          }
          else
            return RC::UNIMPLENMENT;
        }
        break;
        case AggrOp::AGGR_COUNT:{
          std::vector<Value>& index_column_values = AggrValues[i]; 
          int j=index_column_values.size();
          result_cells[i].set_type(INTS);
          result_cells[i].set_int(j);
        }
        break;
        dafault:
        return RC::UNIMPLENMENT;
      }
  }
  if(rc==RC::RECORD_EOF){
    rc=RC::SUCCESS;
  }
  result_tuple_.set_cells(result_cells);
  return rc;
}
RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
Tuple* AggregatePhysicalOperator::current_tuple(){
  return &result_tuple_;
}
void AggregatePhysicalOperator::add_aggregation(const AggrOp aggregation){
    aggregations_.push_back(aggregation);
}