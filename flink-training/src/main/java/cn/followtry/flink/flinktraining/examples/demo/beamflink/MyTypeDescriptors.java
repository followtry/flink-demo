package cn.followtry.flink.flinktraining.examples.demo.beamflink;

import cn.followtry.app.UserInfo;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/30
 */
public class MyTypeDescriptors extends TypeDescriptors {

    public static TypeDescriptor<UserInfo> userInfos() {
        return new PojoTypeDescriptor<UserInfo>() {};
    }
}
