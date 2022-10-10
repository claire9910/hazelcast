package com.hazelcast.alto.engine.iouring;

import com.hazelcast.internal.tpc.iouring.IOUringEventloop;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.alto.engine.EventloopTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class IOUringEventloopTest extends EventloopTest {

    @Override
    public Eventloop createEventloop() {
        return new IOUringEventloop();
    }
}
