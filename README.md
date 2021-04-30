# executor-sync-mysql-slow-query
executor-sync-mysql-slow-query



创建专用账号

```
u_sync_slow_query/xxx
```

授予专用权限

```
GRANT SELECT ON `mysql`.* TO 'u_sync_slow_quer'@'%';
```

