package one.inve.lfn.probe.common.db;

import com.alibaba.druid.pool.DruidDataSource;

public class DataSourceProvider {
	public DruidDataSource getDataSource(String dbId, String dataSourceUrl, String username, String password) {
		try {
			if ("default".equals(dataSourceUrl)) {
				dataSourceUrl = "jdbc:mysql://localhost:3306/main" + dbId
						+ "?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8";
			}

			/**
			 * 数据源
			 */
			DruidDataSource dataSource = new DruidDataSource();
			dataSource.setUrl(dataSourceUrl);
			dataSource.setUsername(username);
			dataSource.setPassword(password);
			dataSource.setDriverClassName("com.mysql.jdbc.Driver");

			// configuration
			dataSource.setInitialSize(1);
			dataSource.setMinIdle(3);
			dataSource.setMaxActive(60000);
			// 配置获取连接等待超时的时间
			dataSource.setMaxWait(60000);
			// 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
			dataSource.setTimeBetweenEvictionRunsMillis(60000);
			// 配置一个连接在池中最小生存的时间，单位是毫秒
			dataSource.setMinEvictableIdleTimeMillis(30000);
			dataSource.setValidationQuery("select 'x'");
			dataSource.setTestWhileIdle(true);
			dataSource.setTestOnBorrow(false);
			dataSource.setTestOnReturn(false);
			// 打开PSCache，并且指定每个连接上PSCache的大小
			dataSource.setPoolPreparedStatements(true);
			dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
			// 配置监控统计拦截的filters，去掉后监控界面sql无法统计，'wall'用于防火墙
			dataSource.setFilters("stat,slf4j");
			// 通过connectProperties属性来打开mergeSql功能；慢SQL记录
			dataSource.setConnectionProperties("druid.stat.mergeSql=true;druid.stat.slowSqlMillis=5000");

			return dataSource;
		} catch (Exception ex) {
			return null;
		}
	}
}
