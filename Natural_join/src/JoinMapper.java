import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{
    private static final String STUDENT_FILENAME = "student.csv";
    private static final String STUDENT_COURSE_FILENAME = "student_course.csv";
    //标签
    private static final String STUDENT_FLAG = "student";
    private static final String STUDENT_COURSE_FLAG = "student_course";

    private FileSplit fileSplit;
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        fileSplit = (FileSplit) context.getInputSplit();
        String filePath = fileSplit.getPath().toString();
        String line = value.toString();
        //将value分为SID和其他属性
        String[] fields = line.split(",", 2);
        //判断记录来自哪个文件
        if (filePath.contains(STUDENT_FILENAME)) {
        	//fields[0]为SID，fields[1]为其他属性
            outKey.set(fields[0]);
            outValue.set(STUDENT_FLAG + " "+ fields[1]);
        }
        else if (filePath.contains(STUDENT_COURSE_FILENAME)) {
            outKey.set(fields[0]);
            outValue.set(STUDENT_COURSE_FLAG + " "+fields[1]);
        }
        context.write(outKey, outValue);
    }
}
