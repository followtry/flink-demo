package cn.followtry.flink.flinktraining.examples.demo.beamflink;

import cn.followtry.app.NameCount;
import cn.followtry.app.UserInfo;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/**
 * 参考自： https://www.e-learn.cn/content/wangluowenzhang/1128944
 * @author jingzhongzhi
 * @Description
 * @since 2019/6/30
 */
public class CntBeanFn extends Combine.AccumulatingCombineFn<UserInfo, CntBeanFn.Accum, NameCount> {

    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Coder<Accum> getAccumulatorCoder(CoderRegistry registry, Coder<UserInfo> inputCoder) throws CannotProvideCoderException {
        return DelegateCoder.of(inputCoder, new DelegateCoder.CodingFunction<Accum, UserInfo>() {
            @Override
            public UserInfo apply(Accum input) throws Exception {
                UserInfo userInfo = new UserInfo();
                return userInfo;
            }
        }, new DelegateCoder.CodingFunction<UserInfo, Accum>() {
            @Override
            public Accum apply(UserInfo input) throws Exception {
                Accum accum = new Accum();
                accum.count = 1;
                return accum;
            }
        });
    }

    @Override
    public TypeDescriptor<UserInfo> getInputType() {
        return super.getInputType();
    }

    @Override
    public TypeDescriptor<NameCount> getOutputType() {
        return super.getOutputType();
    }

    @Override
    public NameCount defaultValue() {
        return super.defaultValue();
    }

    @Override
    public Coder<NameCount> getDefaultOutputCoder(CoderRegistry registry, Coder<UserInfo> inputCoder) throws CannotProvideCoderException {
        return super.getDefaultOutputCoder(registry, inputCoder);
    }

    public static class Accum implements Combine.AccumulatingCombineFn.Accumulator<UserInfo, Accum, NameCount> {

        /**  */
        private Integer count = 0;

        private NameCount nameCount = new NameCount();

        @Override
        public void addInput(UserInfo input) {
            count++;
            nameCount.setCount(count);
            nameCount.setName(input.getName());

        }

        @Override
        public void mergeAccumulator(Accum other) {
            count += other.count;
            nameCount.setCount(count);
        }

        @Override
        public NameCount extractOutput() {
            return nameCount;
        }
    }


    /** A {@link Coder} for a {@link Combine.Holder}. */
    private static class MyCoder<V> extends StructuredCoder<V> {

        private Coder<V> valueCoder;

        public MyCoder(Coder<V> valueCoder) {
            this.valueCoder = valueCoder;
        }

        @Override
        public void encode(V accumulator, OutputStream outStream)
                throws CoderException, IOException {
            encode(accumulator, outStream, Context.NESTED);
        }

        @Override
        public void encode(V accumulator, OutputStream outStream, Coder.Context context)
                throws CoderException, IOException {
            valueCoder.encode(accumulator, outStream, context);
        }

        @Override
        public V decode(InputStream inStream) throws CoderException, IOException {
            return decode(inStream, Context.NESTED);
        }

        @Override
        public V decode(InputStream inStream, Coder.Context context)
                throws CoderException, IOException {
            return valueCoder.decode(inStream, context);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.singletonList(valueCoder);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            valueCoder.verifyDeterministic();
        }
    }

    public static final class ToNameCountCodingFunction implements CodingFunction2<Accum, NameCount> {

        @Override
        public NameCount apply(Accum input) throws Exception {
            NameCount nameCount = new NameCount();
            int cnt = nameCount.getCount() + input.count;
            nameCount.setCount(cnt);
            return nameCount;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ToNameCountCodingFunction;
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }
    }

    public static final class FromUserInfoCodingFunction implements CodingFunction2<UserInfo,Accum> {

        @Override
        public Accum apply(UserInfo input) throws Exception {
            Accum accum = new Accum();
            accum.count = 1;
            return accum;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof FromUserInfoCodingFunction;
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }
    }

}
