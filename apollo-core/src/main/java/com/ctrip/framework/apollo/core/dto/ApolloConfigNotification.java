package com.ctrip.framework.apollo.core.dto;

/**
 * 按道理说，对于一个 Namespace 的通知，使用 ApolloConfigNotification 的 namespaceName + notificationId 已经足够了。
 * 但是，在 namespaceName 对应的 Namespace 是关联类型时，会同时查询当前 Namespace + 关联的 Namespace 这两个 Namespace，
 * 所以会是多个，使用 Map 数据结构。
 * @author Jason Song(song_s@ctrip.com)
 */
public class ApolloConfigNotification {
  /**
   * Namespace 名字
   */
  private String namespaceName;
  /**
   * 最新通知编号
   *
   * 目前使用 `ReleaseMessage.id` 。
   */
  private long notificationId;
  /**
   * 通知消息集合
   */
  private volatile ApolloNotificationMessages messages;

  //for json converter
  public ApolloConfigNotification() {
  }

  public ApolloConfigNotification(String namespaceName, long notificationId) {
    this.namespaceName = namespaceName;
    this.notificationId = notificationId;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public long getNotificationId() {
    return notificationId;
  }

  public void setNamespaceName(String namespaceName) {
    this.namespaceName = namespaceName;
  }

  public ApolloNotificationMessages getMessages() {
    return messages;
  }

  public void setMessages(ApolloNotificationMessages messages) {
    this.messages = messages;
  }

  public void addMessage(String key, long notificationId) {
    // 创建 ApolloNotificationMessages 对象
    if (this.messages == null) {
      synchronized (this) {
        if (this.messages == null) {
          this.messages = new ApolloNotificationMessages();
        }
      }
    }
    // 添加到 `messages` 中
    this.messages.put(key, notificationId);
  }

  @Override
  public String toString() {
    return "ApolloConfigNotification{" +
        "namespaceName='" + namespaceName + '\'' +
        ", notificationId=" + notificationId +
        '}';
  }
}
