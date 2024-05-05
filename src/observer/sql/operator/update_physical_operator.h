#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "sql/expr/tuple.h"
class Updatestmt;

class UpdatePhysicalOperator :public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table,Field field,Value value):table_(table),field_(field),value_(value){};
  virtual ~UpdatePhysicalOperator()=default;
  PhysicalOperatorType type()const override
  {
    return PhysicalOperatorType::INSERT;
  }
  RC open(Trx *trx)override;
  RC next()override;
  RC close()override;
  Tuple *current_tuple()override { return nullptr; }
private:
  Table *table_= nullptr;
  Field field_;
  Value value_;
  Trx *trx_=nullptr;
};