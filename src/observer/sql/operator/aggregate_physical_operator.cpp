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
  for(int i=0;i<number;i++){
    const AggrOp aggregation =aggregations_[i];
    if(aggregation==AggrOp::AGGR_SUM){
      result_cells[i].set_type(AttrType::FLOATS);
      result_cells[i].set_float(0.0);
    }
    else if(aggregation==AggrOp::AGGR_MIN){
      result_cells[i].set_type(AttrType::FLOATS);
      result_cells[i].set_float(0.0);
    }
    else if(aggregation==AggrOp::AGGR_MAX){
      result_cells[i].set_type(AttrType::FLOATS);
      result_cells[i].set_float(0.0);
    }
    else if(aggregation==AggrOp::AGGR_AVG){
      result_cells[i].set_type(AttrType::FLOATS);
      result_cells[i].set_float(0.0);
    }
    else if(aggregation==AggrOp::AGGR_COUNT){
      result_cells[i].set_type(AttrType::INTS);
      result_cells[i].set_float(0);
    }
  }
  int count=0;
  while(RC::SUCCESS==(rc=oper->next())){
    Tuple*tuple=oper->current_tuple();
    for(int cell_idx=0;cell_idx<(int)aggregations_.size();cell_idx++){
        const AggrOp aggregation=aggregations_[cell_idx];
        Value cell;
        AttrType attr_type=AttrType::INTS;
        switch(aggregation){
            case AggrOp::AGGR_SUM:
              rc=tuple->cell_at(cell_idx,cell);
              attr_type=cell.attr_type();
              if(attr_type==AttrType::INTS or attr_type==AttrType::FLOATS){
                  result_cells[cell_idx].set_float(result_cells[cell_idx].get_float()+cell.get_float());
              }
              break;
            case AggrOp::AGGR_MIN:
              rc=tuple->cell_at(cell_idx,cell);
              attr_type=cell.attr_type();
              if(result_cells[cell_idx].compare(cell)>0){
                switch(attr_type){
                  case INTS:
                    result_cells[cell_idx].set_int(cell.get_int());
                  break;
                  case FLOATS:
                    result_cells[cell_idx].set_float(cell.get_float());
                  break;
                  case CHARS:
                    result_cells[cell_idx].set_string(cell.get_string().data());
                  break;
                  case DATES:
                    result_cells[cell_idx].set_date(cell.get_date());
                  break;
                  default:
                    return RC::UNIMPLENMENT;
                }
              }
              break;
            case AggrOp::AGGR_MAX:
              rc=tuple->cell_at(cell_idx,cell);
              attr_type=cell.attr_type();
              if(result_cells[cell_idx].compare(cell)<0){
                switch(attr_type){
                  case INTS:
                    result_cells[cell_idx].set_int(cell.get_int());
                  break;
                  case FLOATS:
                    result_cells[cell_idx].set_float(cell.get_float());
                  break;
                  case CHARS:
                    result_cells[cell_idx].set_string(cell.get_string().data());
                  break;
                  case DATES:
                    result_cells[cell_idx].set_date(cell.get_date());
                  break;
                  default:
                    return RC::UNIMPLENMENT;
                }
              }
              break;
            case AggrOp::AGGR_AVG:
              rc = tuple->cell_at(cell_idx, cell);  
              attr_type = cell.attr_type();   
              if (attr_type == AttrType::INTS || attr_type == AttrType::FLOATS) {   
                static double sum = 0.0;  
                static int count = 0;   
                sum += cell.get_float();  
                count++;   
                if (count > 0) {  
                  result_cells[cell_idx].set_float(sum / count);  
                }  
              } 
              break;
            case AggrOp::AGGR_COUNT:
              rc=tuple->cell_at(cell_idx,cell);
              attr_type=cell.attr_type();
              
              break;
            default:
              return RC::UNIMPLENMENT;
        }
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