package watch;

import java.io.File;

/**
 * 邮件监听器
 * 收到通知后发送邮件
 *
 * @author debao.yang
 * @since 2024/7/6 11:27
 */
public class EmailNotificationListener implements EventListener {

    private String email;

    public EmailNotificationListener(String emial) {
        this.email = emial;
    }

    @Override
    public void update(String eventType, File file) {
        System.out.println("Email to" + email + ": Someone has performed" + eventType + "operation with the following file: " + file.getName());
    }
}
