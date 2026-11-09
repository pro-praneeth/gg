set -e

echo "="
echo " Setting Cloudera / CDH Environment Variables "
echo "="

export JAVA_HOME=${JAVA_HOME:-/usr/java/default}
export HADOOP_HOME=${HADOOP_HOME:-/usr/lib/hadoop}
export PATH=$PATH:$JAVA_HOME/bin:/usr/bin

if ! command -v javac &> /dev/null; then
echo "Notice: Standard javac not in path. Attempting to fall back to Cloudera JDK path..."
export PATH=$PATH:/usr/java/default/bin
fi

JAVA_FILE="WordCount.java"
CLASS_NAME="WordCount"
JAR_NAME="wordcount.jar"

LOCAL_INPUT_DIR="./wc_local_input"
LOCAL_FILE="${LOCAL_INPUT_DIR}/sample.txt"

HDFS_INPUT_DIR="/user/cloudera/wordcount/input"
HDFS_OUTPUT_DIR="/user/cloudera/wordcount/output"

echo "="
echo " Starting Monolithic MapReduce WordCount Process"
echo "="

echo "--> Generating Java Source Code File (${JAVA_FILE})..."
cat << 'EOF' > "${JAVA_FILE}"
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String cleanWord = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
            if (!cleanWord.isEmpty()) {
                word.set(cleanWord);
                context.write(word, one);
            }
        }
    }
}

public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}

public static void main(String[] args) throws Exception {
    if (args.length != 2) {
        System.err.println("Usage: WordCount <input path> <output path>");
        System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "cloudera word count");

    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // FIXED: Passing individual string indices instead of the full args array
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
EOF

echo "--> Compiling ${JAVA_FILE}..."
javac -cp $(hadoop classpath) ${JAVA_FILE}

echo "--> Creating JAR file package (${JAR_NAME})..."
jar cf ${JAR_NAME} ${CLASS_NAME}*.class

rm -f ${CLASS_NAME}*.class
rm -f ${JAVA_FILE}
echo "JAR successfully built. Source artifacts cleaned."

echo "--> Generating local sample text..."
rm -rf "${LOCAL_INPUT_DIR}"
mkdir -p "${LOCAL_INPUT_DIR}"

cat << 'EOF' > "${LOCAL_FILE}"
Hadoop is an open-source framework.
MapReduce is core to Hadoop.
This Java program counts every word in this file.
Hadoop MapReduce is efficient and scalable.
EOF

echo "--> Cleaning up existing paths in HDFS..."
hadoop fs -rm -r -f ${HDFS_INPUT_DIR} || true
hadoop fs -rm -r -f ${HDFS_OUTPUT_DIR} || true

echo "--> Provisioning HDFS input directory and uploading files..."
hadoop fs -mkdir -p ${HDFS_INPUT_DIR}
hadoop fs -put "${LOCAL_FILE}" ${HDFS_INPUT_DIR}/

echo "--> Launching custom MapReduce Job..."
hadoop jar ${JAR_NAME} ${CLASS_NAME} ${HDFS_INPUT_DIR} ${HDFS_OUTPUT_DIR}

rm -rf "${LOCAL_INPUT_DIR}"
rm -f ${JAR_NAME}

echo "="
echo " Execution Completed Successfully! Output Results:"
echo "="
hadoop fs -cat ${HDFS_OUTPUT_DIR}/part-r-00000
