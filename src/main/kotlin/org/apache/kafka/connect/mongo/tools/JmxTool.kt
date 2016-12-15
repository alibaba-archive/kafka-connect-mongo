package org.apache.kafka.connect.mongo.tools

import java.lang.management.ManagementFactory
import javax.management.ObjectName

/**
 * @author Xu Jingxin
 */

class JmxTool {
    companion object {
        private var incr = 0
        fun registerMBean(instance: Any) {
            synchronized(JmxTool) {
                val mbs = ManagementFactory.getPlatformMBeanServer()
                val name = ObjectName("org.apache.kafka.connect.mongo:type=${instance.javaClass.name}-${++incr}")
                mbs.registerMBean(instance, name)
            }
        }
    }
}
