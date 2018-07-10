import java.text.DecimalFormat;

/**
 * Created by X.Y on 2018/6/28
 */
public class test {
    public static void main(String[] args) {
        double now=4.96;
        int count=0;

        DecimalFormat df = new DecimalFormat(".00");

        while(now<20){
            now=now*1.1;
            count++;
            System.out.println("***************************");
            System.out.println("price="+df.format(now));
            System.out.println("count="+count);
        }

    }

}
