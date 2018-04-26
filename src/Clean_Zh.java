import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Clean_Zh {
    /**
     * 清理文件(中文乱码问题)
     * 处理GB2312编码问题，将终端中的输出保存文件为outputFileName
     * @param inputFileName 待清理的文件名
     * @param outputFileName 待清理的文件名
     * @throws IOException IO
     */
    private static void cleanFile(String inputFileName, String outputFileName) throws IOException {
        // 将控制台的输出 保存为文件
        PrintStream out = new PrintStream(outputFileName);
        System.setOut(out);
        // 将控制台的输出 保存为文件

        FileInputStream fis = new FileInputStream(inputFileName);
        // 得到原始文件编码 getCharset(inputFileName)，解决中文乱码
        InputStreamReader isr = new InputStreamReader(fis, getCharset(inputFileName));
        BufferedReader br = new BufferedReader(isr);
        try {
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = br.readLine()) != null) {
                System.out.println(tempString);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * 给定文件，判断其编码格式
     * @param fileName 文件名
     */
    private static String getCharset(String fileName) throws IOException {

        BufferedInputStream bin = new BufferedInputStream(new FileInputStream(fileName));
        int p = (bin.read() << 8) + bin.read();

        String code = null;

        switch (p) {
            case 0xefbb:
                code = "UTF-8";
                break;
            case 0xfffe:
                code = "Unicode";
                break;
            case 0xfeff:
                code = "UTF-16BE";
                break;
            default:
                code = "GBK";
        }
        return code;
    }

    /**
     * 得到fileDir下所有的文件
     */
    private static List<File> getFileFromDic(String fileDir) {
        List<File> fileList = new ArrayList<File>();
        File file = new File(fileDir);
        File[] files = file.listFiles();// 获取目录下的所有文件或文件夹
        if (files == null) {// 如果目录为空，直接退出
            System.out.println("目录为空");
            return null;
        }
        // 遍历，目录下的所有文件
        for (File f : files) {
            if (f.isFile()) {
                fileList.add(f);
            } else if (f.isDirectory()) {
                System.out.println(f.getAbsolutePath());
                getFileFromDic(f.getAbsolutePath());
            }
        }
        return fileList;
    }

    /**
     * 文件编码清理
     * @param inputDic 待清理的文件夹(本地)
     * @param outputDic 清理后的文件夹(本地)
     * @throws IOException IO
     */
    private static void clean_Dir(String inputDic, String outputDic) throws IOException {
        List<File> inputFileList = getFileFromDic(inputDic);
        List<String> fileName = new ArrayList<String>();
        assert inputFileList != null;
        for (File f1 : inputFileList) {
            fileName.add(f1.getName());
        }
        // 打印出inputDic下所有的文件名
        for (String aFileName : fileName) {
            System.out.println(aFileName);
        }
        // 处理GB2312编码问题，将终端中的输出保存文件为outputFileName
        for (String aFileName : fileName) {
            String inputFileName = inputDic + "/" + aFileName;
            String outputFileName = outputDic + "/" + aFileName;
            cleanFile(inputFileName, outputFileName);
        }
    }

    public static void main(String[] args) throws IOException {
        String inputDic = "/Users/zhaoxuyan/Desktop/export_Zh";
        String outputDic = "/Users/zhaoxuyan/Desktop/export_Zh_clean";
        clean_Dir(inputDic, outputDic);
    }
}