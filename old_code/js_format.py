# coding:utf-8
with open('js_code.js', 'rb') as f:
  code = f.read().decode('utf-8')
lines = code.split(";")
# 一般压缩后的文件所有代码都在一行里
# 视情况设定索引，我的情况时第0行是源代码。
indent = 0
formatted = []
for line in lines:
  newline = []
  for char in line:
    newline.append(char)
    if char=='{': #{ 是缩进的依据
      indent+=1
      newline.append("\n")
      newline.append("\t"*indent)
    if char=="}":
      indent-=1
      newline.append("\n")
      newline.append("\t"*indent)
  formatted.append("\t"*indent+"".join(newline))
open("js_code_formated.js", "w").writelines(";\n".join(formatted))