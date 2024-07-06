package watch;

import java.io.File;

/**
 * 日志监听器
 * 收到通知后发送一条日志
 *
 * @author debao.yang
 * @since 2024/7/6 11:30
 */
public class LogOpenListener implements EventListener {

    private File log;

    public LogOpenListener(String fileName) {
        this.log = new File(fileName);
    }

    @Override
    public void update(String eventType, File file) {
        System.out.println("Save to log" + log + ": Someone has performed " + eventType + " operation with the following file:" + file.getName());
    }
}
