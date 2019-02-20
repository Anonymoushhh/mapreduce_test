import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text>{
    private static final String STUDENT_FLAG = "student";
    private static final String STUDENT_COURSE_FLAG = "student_course";
    private String fileFlag = null;
    private List<String> record=null;
    private Text Value = new Text();
    private String stu=null;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        record = new ArrayList<>();
        for (Text val : values) {
            String[] fields = StringUtils.split(val.toString()," ");
            fileFlag = fields[0];
            //判断记录来自哪个文件，并根据文件格式解析记录。
            if (fileFlag.equals(STUDENT_FLAG)) {
                stu=fields[1];
            }
            else if (fileFlag.equals(STUDENT_COURSE_FLAG)) {
                record.add(fields[1]);
            }
        }
        //求笛卡尔积
        for (String i:record) {
            Value.set(i+","+stu);
            context.write(key, Value);
        }
    }

}