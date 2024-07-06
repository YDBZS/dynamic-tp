package watch;

import java.io.File;

/**
 * 事件监听器
 *
 * @author debao.yang
 * @since 2024/7/6 11:08
 */
public interface EventListener {

    void update(String eventType, File file);

}
