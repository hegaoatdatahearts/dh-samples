package com.datahearts.dbsuport;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by gaohe on 16/9/14.
 */
public class DBLoadConfiguration extends Configuration {
    static {
        Configuration.addDefaultResource("oracle-setting.xml");
//        Configuration.addDefaultResource("mysql-setting.xml");
//        Configuration.addDefaultResource("mysql-setting.xml");

    }
}
