package com.ctrip.framework.apollo.portal.service;

import com.ctrip.framework.apollo.common.entity.App;

public interface RoleInitializationService {

  /**
   * 初始化 App 级的 Role
   */
  public void initAppRoles(App app);

  /**
   * 初始化 Namespace 级的 Role
   */
  public void initNamespaceRoles(String appId, String namespaceName, String operator);

  public void initNamespaceEnvRoles(String appId, String namespaceName, String operator);

  public void initNamespaceSpecificEnvRoles(String appId, String namespaceName, String env, String operator);

}
