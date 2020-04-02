import java.io.*;
import java.util.Scanner;

public class Solution {
    public static void main(String[] args) throws IOException {
        File file = new File("user_artists.dat");
        Scanner sc = new Scanner(new FileInputStream(file));
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("user_artist_small.dat")));
        String[] s = sc.nextLine().split("\t");
        int c=0,uid=Integer.parseInt(s[0]);
        while (sc.hasNext())
        {
            s = sc.nextLine().split("\t");
            if (c<10)
            {
                c++;
                bw.write(s[0]+"\t"+s[1]+"\t"+s[2]+"\n");
                bw.flush();
            }
            if (uid!=Integer.parseInt(s[0])) {
                c = 0;
                uid = Integer.parseInt(s[0]);
            }
        }
    }
}
