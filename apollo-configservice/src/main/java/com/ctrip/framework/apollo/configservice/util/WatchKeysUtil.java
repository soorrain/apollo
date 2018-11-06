package com.ctrip.framework.apollo.configservice.util;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import com.ctrip.framework.apollo.configservice.service.AppNamespaceServiceWithCache;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.core.ConfigConsts;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class WatchKeysUtil {
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  @Autowired
  private AppNamespaceServiceWithCache appNamespaceService;

  /**
   * Assemble watch keys for the given appId, cluster, namespace, dataCenter combination
   */
  public Set<String> assembleAllWatchKeys(String appId, String clusterName, String namespace,
                                          String dataCenter) {
    Multimap<String, String> watchedKeysMap =
        assembleAllWatchKeys(appId, clusterName, Sets.newHashSet(namespace), dataCenter);
    return Sets.newHashSet(watchedKeysMap.get(namespace));
  }

  /**
   * 组装所有的 Watch Key Multimap 。其中 Key 为 Namespace 的 名字，Value 为 Watch Key 集合。
   * Assemble watch keys for the given appId, cluster, namespaces, dataCenter combination
   *
   * @return a multimap with namespace as the key and watch keys as the value
   */
  public Multimap<String, String> assembleAllWatchKeys(String appId, String clusterName,
                                                       Set<String> namespaces,
                                                       String dataCenter) {
    // 组装 Watch Key Multimap
    Multimap<String, String> watchedKeysMap =
        assembleWatchKeys(appId, clusterName, namespaces, dataCenter);

    // 如果不是仅监听 `application` Namespace，处理其关联来的 Namespace
    //Every app has an 'application' namespace
    if (!(namespaces.size() == 1 && namespaces.contains(ConfigConsts.NAMESPACE_APPLICATION))) {
      // 获得属于该 App 的 Namespace 的名字集合
      Set<String> namespacesBelongToAppId = namespacesBelongToAppId(appId, namespaces);
      // 获得关联来的 Namespace 的名字的集合
      Set<String> publicNamespaces = Sets.difference(namespaces, namespacesBelongToAppId);

      // 添加到 Watch Key Multimap 中
      //Listen on more namespaces if it's a public namespace
      if (!publicNamespaces.isEmpty()) {
        watchedKeysMap
            .putAll(findPublicConfigWatchKeys(appId, clusterName, publicNamespaces, dataCenter));
      }
    }

    return watchedKeysMap;
  }

  /**
   * 获得 Namespace 类型为 public 对应的 Watch Key Multimap
   * 重要：要求非当前 App 的 Namespace
   *
   * @param applicationId
   * @param clusterName
   * @param namespaces
   * @param dataCenter
   * @return
   */
  private Multimap<String, String> findPublicConfigWatchKeys(String applicationId,
                                                             String clusterName,
                                                             Set<String> namespaces,
                                                             String dataCenter) {
    Multimap<String, String> watchedKeysMap = HashMultimap.create();
    // 获得 Namespace 为 Public 的 AppNamespace 数组
    List<AppNamespace> appNamespaces = appNamespaceService.findPublicNamespacesByNames(namespaces);

    for (AppNamespace appNamespace : appNamespaces) {
      //check whether the namespace's appId equals to current one
      if (Objects.equals(applicationId, appNamespace.getAppId())) {
        continue;
      }

      String publicConfigAppId = appNamespace.getAppId();

      watchedKeysMap.putAll(appNamespace.getName(),
          assembleWatchKeys(publicConfigAppId, clusterName, appNamespace.getName(), dataCenter));
    }

    return watchedKeysMap;
  }

  private String assembleKey(String appId, String cluster, String namespace) {
    return STRING_JOINER.join(appId, cluster, namespace);
  }

  private Set<String> assembleWatchKeys(String appId, String clusterName, String namespace,
                                        String dataCenter) {
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return Collections.emptySet();
    }
    Set<String> watchedKeys = Sets.newHashSet();

    // 指定 Cluster
    //watch specified cluster config change
    if (!Objects.equals(ConfigConsts.CLUSTER_NAME_DEFAULT, clusterName)) {
      watchedKeys.add(assembleKey(appId, clusterName, namespace));
    }

    // 所属 IDC 的 Cluster
    //watch data center config change
    if (!Strings.isNullOrEmpty(dataCenter) && !Objects.equals(dataCenter, clusterName)) {
      watchedKeys.add(assembleKey(appId, dataCenter, namespace));
    }

    // 默认 Cluster
    //watch default cluster config change
    watchedKeys.add(assembleKey(appId, ConfigConsts.CLUSTER_NAME_DEFAULT, namespace));

    return watchedKeys;
  }

  private Multimap<String, String> assembleWatchKeys(String appId, String clusterName,
                                                     Set<String> namespaces,
                                                     String dataCenter) {
    Multimap<String, String> watchedKeysMap = HashMultimap.create();

    // 循环 Namespace 的名字集合
    for (String namespace : namespaces) {
      watchedKeysMap
          .putAll(namespace, assembleWatchKeys(appId, clusterName, namespace, dataCenter));
    }

    return watchedKeysMap;
  }

  private Set<String> namespacesBelongToAppId(String appId, Set<String> namespaces) {
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return Collections.emptySet();
    }
    // 获取属于该 App 的 AppNamespace 集合
    List<AppNamespace> appNamespaces =
        appNamespaceService.findByAppIdAndNamespaces(appId, namespaces);

    if (appNamespaces == null || appNamespaces.isEmpty()) {
      return Collections.emptySet();
    }

    // 返回 AppNamespace 的名字的集合
    return FluentIterable.from(appNamespaces).transform(AppNamespace::getName).toSet();
  }
}
