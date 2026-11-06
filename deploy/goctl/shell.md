goctl model pg datasource 
  -url "postgres://user:pass@127.0.0.1:5432/postgres?sslmode=disable" 
  -s public 
  -t device 
  -dir app/iotdevice/model 
  --home deploy/goctl
