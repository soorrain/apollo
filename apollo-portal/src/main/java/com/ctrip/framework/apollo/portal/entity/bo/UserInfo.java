package com.ctrip.framework.apollo.portal.entity.bo;

public class UserInfo {

  /**
   * 账号 {@link com.ctrip.framework.apollo.portal.entity.po.UserPO#username}
   */
  private String userId;
  /**
   * 账号 {@link com.ctrip.framework.apollo.portal.entity.po.UserPO#username}
   */
  private String name;
  /**
   * 邮箱 {@link com.ctrip.framework.apollo.portal.entity.po.UserPO#email}
   */
  private String email;

  public UserInfo() {

  }

  public UserInfo(String userId) {
    this.userId = userId;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof UserInfo) {

      if (o == this){
        return true;
      }

      UserInfo anotherUser = (UserInfo) o;
      return userId.equals(anotherUser.userId);
    } else {
      return false;
    }

  }
}
