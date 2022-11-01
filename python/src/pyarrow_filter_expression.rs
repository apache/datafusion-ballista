// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Converts a Datafusion logical plan expression (Expr) into a PyArrow compute expression
use pyo3::prelude::*;

use std::convert::TryFrom;
use std::result::Result;

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::*;

use crate::errors::DataFusionError;

#[derive(Debug, Clone)]
#[repr(transparent)]
pub(crate) struct PyArrowFilterExpression(PyObject);

fn operator_to_py<'py>(
    operator: &Operator,
    op: &'py PyModule,
) -> Result<&'py PyAny, DataFusionError> {
    let py_op: &PyAny = match operator {
        Operator::Eq => op.getattr("eq")?,
        Operator::NotEq => op.getattr("ne")?,
        Operator::Lt => op.getattr("lt")?,
        Operator::LtEq => op.getattr("le")?,
        Operator::Gt => op.getattr("gt")?,
        Operator::GtEq => op.getattr("ge")?,
        Operator::And => op.getattr("and_")?,
        Operator::Or => op.getattr("or_")?,
        _ => {
            return Err(DataFusionError::Common(format!(
                "Unsupported operator {:?}",
                operator
            )))
        }
    };
    Ok(py_op)
}

fn extract_scalar_list(
    exprs: &[Expr],
    py: Python,
) -> Result<Vec<PyObject>, DataFusionError> {
    let ret: Result<Vec<PyObject>, DataFusionError> = exprs
        .iter()
        .map(|expr| match expr {
            Expr::Literal(v) => match v {
                ScalarValue::Boolean(Some(b)) => Ok(b.into_py(py)),
                ScalarValue::Int8(Some(i)) => Ok(i.into_py(py)),
                ScalarValue::Int16(Some(i)) => Ok(i.into_py(py)),
                ScalarValue::Int32(Some(i)) => Ok(i.into_py(py)),
                ScalarValue::Int64(Some(i)) => Ok(i.into_py(py)),
                ScalarValue::UInt8(Some(i)) => Ok(i.into_py(py)),
                ScalarValue::UInt16(Some(i)) => Ok(i.into_py(py)),
                ScalarValue::UInt32(Some(i)) => Ok(i.into_py(py)),
                ScalarValue::UInt64(Some(i)) => Ok(i.into_py(py)),
                ScalarValue::Float32(Some(f)) => Ok(f.into_py(py)),
                ScalarValue::Float64(Some(f)) => Ok(f.into_py(py)),
                ScalarValue::Utf8(Some(s)) => Ok(s.into_py(py)),
                _ => Err(DataFusionError::Common(format!(
                    "PyArrow can't handle ScalarValue: {:?}",
                    v
                ))),
            },
            _ => Err(DataFusionError::Common(format!(
                "Only a list of Literals are supported got {:?}",
                expr
            ))),
        })
        .collect();
    ret
}

impl PyArrowFilterExpression {
    pub fn inner(&self) -> &PyObject {
        &self.0
    }
}

impl TryFrom<&Expr> for PyArrowFilterExpression {
    type Error = DataFusionError;

    // Converts a Datafusion filter Expr into an expression string that can be evaluated by Python
    // Note that pyarrow.compute.{field,scalar} are put into Python globals() when evaluated
    // isin, is_null, and is_valid (~is_null) are methods of pyarrow.dataset.Expression
    // https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html#pyarrow-dataset-expression
    fn try_from(expr: &Expr) -> Result<Self, Self::Error> {
        Python::with_gil(|py| {
            let pc = Python::import(py, "pyarrow.compute")?;
            let op_module = Python::import(py, "operator")?;
            let pc_expr: Result<&PyAny, DataFusionError> = match expr {
                Expr::Column(Column { name, .. }) => {
                    Ok(pc.getattr("field")?.call1((name,))?)
                }
                Expr::Literal(v) => match v {
                    ScalarValue::Boolean(Some(b)) => {
                        Ok(pc.getattr("scalar")?.call1((*b,))?)
                    }
                    ScalarValue::Int8(Some(i)) => {
                        Ok(pc.getattr("scalar")?.call1((*i,))?)
                    }
                    ScalarValue::Int16(Some(i)) => {
                        Ok(pc.getattr("scalar")?.call1((*i,))?)
                    }
                    ScalarValue::Int32(Some(i)) => {
                        Ok(pc.getattr("scalar")?.call1((*i,))?)
                    }
                    ScalarValue::Int64(Some(i)) => {
                        Ok(pc.getattr("scalar")?.call1((*i,))?)
                    }
                    ScalarValue::UInt8(Some(i)) => {
                        Ok(pc.getattr("scalar")?.call1((*i,))?)
                    }
                    ScalarValue::UInt16(Some(i)) => {
                        Ok(pc.getattr("scalar")?.call1((*i,))?)
                    }
                    ScalarValue::UInt32(Some(i)) => {
                        Ok(pc.getattr("scalar")?.call1((*i,))?)
                    }
                    ScalarValue::UInt64(Some(i)) => {
                        Ok(pc.getattr("scalar")?.call1((*i,))?)
                    }
                    ScalarValue::Float32(Some(f)) => {
                        Ok(pc.getattr("scalar")?.call1((*f,))?)
                    }
                    ScalarValue::Float64(Some(f)) => {
                        Ok(pc.getattr("scalar")?.call1((*f,))?)
                    }
                    ScalarValue::Utf8(Some(s)) => {
                        Ok(pc.getattr("scalar")?.call1((s,))?)
                    }
                    _ => Err(DataFusionError::Common(format!(
                        "PyArrow can't handle ScalarValue: {:?}",
                        v
                    ))),
                },
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    let operator = operator_to_py(op, op_module)?;
                    let left = PyArrowFilterExpression::try_from(left.as_ref())?.0;
                    let right = PyArrowFilterExpression::try_from(right.as_ref())?.0;
                    Ok(operator.call1((left, right))?)
                }
                Expr::Not(expr) => {
                    let operator = op_module.getattr("invert")?;
                    let py_expr = PyArrowFilterExpression::try_from(expr.as_ref())?.0;
                    Ok(operator.call1((py_expr,))?)
                }
                Expr::IsNotNull(expr) => {
                    let py_expr = PyArrowFilterExpression::try_from(expr.as_ref())?
                        .0
                        .into_ref(py);
                    Ok(py_expr.call_method0("is_valid")?)
                }
                Expr::IsNull(expr) => {
                    let expr = PyArrowFilterExpression::try_from(expr.as_ref())?
                        .0
                        .into_ref(py);
                    Ok(expr.call_method1("is_null", (expr,))?)
                }
                Expr::Between(Between {
                    expr,
                    negated,
                    low,
                    high,
                }) => {
                    let expr = PyArrowFilterExpression::try_from(expr.as_ref())?.0;
                    let low = PyArrowFilterExpression::try_from(low.as_ref())?.0;
                    let high = PyArrowFilterExpression::try_from(high.as_ref())?.0;
                    let and = op_module.getattr("and_")?;
                    let le = op_module.getattr("le")?;
                    let invert = op_module.getattr("invert")?;

                    // scalar <= field() returns a boolean expression so we need to use and to combine these
                    let ret = and.call1((
                        le.call1((low, expr.clone_ref(py)))?,
                        le.call1((expr, high))?,
                    ))?;

                    Ok(if *negated { invert.call1((ret,))? } else { ret })
                }
                Expr::InList {
                    expr,
                    list,
                    negated,
                } => {
                    let expr = PyArrowFilterExpression::try_from(expr.as_ref())?
                        .0
                        .into_ref(py);
                    let scalars = extract_scalar_list(list, py)?;
                    let ret = expr.call_method1("isin", (scalars,))?;
                    let invert = op_module.getattr("invert")?;

                    Ok(if *negated { invert.call1((ret,))? } else { ret })
                }
                _ => Err(DataFusionError::Common(format!(
                    "Unsupported Datafusion expression {:?}",
                    expr
                ))),
            };
            Ok(PyArrowFilterExpression(pc_expr?.into()))
        })
    }
}
