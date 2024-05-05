/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai on 2021/6/11.
//

#include <string.h>
#include <algorithm>
#include "common/defs.h"
#include <climits>

namespace common {


int compare_int(void *arg1, void *arg2)
{
  int v1 = *(int *)arg1;
  int v2 = *(int *)arg2;
  return v1 - v2;
}

int compare_date(void*arg1,void*arg2){
  int v1 = *(int *)arg1;
  int v2 = *(int *)arg2;
  return v1 - v2;
}
int compare_float(void *arg1, void *arg2)
{
  float v1 = *(float *)arg1;
  float v2 = *(float *)arg2;
  float cmp = v1 - v2;
  if (cmp > EPSILON) {
    return 1;
  }
  if (cmp < -EPSILON) {
    return -1;
  }
  return 0;
}

int compare_string(void *arg1, int arg1_max_length, void *arg2, int arg2_max_length)
{
  const char *s1 = (const char *)arg1;
  const char *s2 = (const char *)arg2;
  int maxlen = std::min(arg1_max_length, arg2_max_length);
  int result = strncmp(s1, s2, maxlen);
  if (0 != result) {
    return result;
  }

  if (arg1_max_length > maxlen) {
    return s1[maxlen] - 0;
  }

  if (arg2_max_length > maxlen) {
    return 0 - s2[maxlen];
  }
  return 0;
}

int compare_str_with_int(void*arg1,int arg1_max_length,void*arg2){
    const char* s1 = (const char*)arg1;  
    int v2 = *(int*)arg2;  
    if (s1 == NULL || arg1_max_length <= 0) {   
        return -1;  
    }  
 
    char *endptr;  
    errno = 0; 
    long long v1 = strtol(s1, &endptr, 10);  
  
    // 检查是否转换了整个字符串，并且没有溢出  
    if ((errno == ERANGE && (v1 == LLONG_MAX || v1 == LLONG_MIN))  
        || (endptr == s1) // 没有转换任何字符  
        || (*endptr != '\0' && !isspace((unsigned char)*endptr))) { // 字符串包含非数字字符  

        return -1;  
    }  
  
    // 检查是否超过了 int 的范围  
    if (v1 > INT_MAX || v1 < INT_MIN) {   
        return -1;  
    }   
    int result = (int)v1 - v2;  
    if (result > 0) {  
        return 1;  
    }  
    if (result < 0) {  
        return -1;  
    }  
    return 0;  
}
float fabs(float x) {  
    return (x >= 0.0f) ? x : -x;  
}
int compare_str_with_float(void *arg1, int arg1_max_length, float *arg2) {  
    const char *s1 = (const char *)arg1;  
    float v2 = *arg2;  
    float v1 = atof(s1);  
      
    if (s1 == NULL) {   
        return -1;  
    }  
  
    // 使用 fabs 比较浮点数的绝对值与 EPSILON  
    float diff = fabs(v1 - v2);  
    if (diff > EPSILON) {  
        // 如果差值大于 EPSILON，则 v1 和 v2 不相等  
        return (v1 > v2) ? 1 : -1; // 直接返回大于、小于的结果  
    }  
  
    // 如果差值在 EPSILON 之内，则认为它们相等  
    return 0; // v1 等于 v2（在允许的精度范围内）  
}

}
