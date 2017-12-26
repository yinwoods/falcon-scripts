### 监控nginx

** 使用说明：https://github.com/GuyCheung/falcon-ngx_metric **

在该目录下新建config.ini，并写入以下内容

[nginx]
url = nginx 收集url

[agent]
service = open-falcon endpoint
format = falcon
ngx_out_sep = |
agent_addr = agent api地址
falcon_step = 60
