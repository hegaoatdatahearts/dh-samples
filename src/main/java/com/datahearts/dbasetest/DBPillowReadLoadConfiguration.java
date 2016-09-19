package com.datahearts.dbasetest;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by gaohe on 16/9/14.
 */
public class DBPillowReadLoadConfiguration extends Configuration {
    static {
        Configuration.addDefaultResource("dbase-setting.xml");

    }
}
