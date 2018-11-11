package com.ctrip.framework.apollo.biz.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import com.ctrip.framework.apollo.biz.entity.Audit;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.repository.ReleaseRepository;
import com.ctrip.framework.apollo.biz.utils.ReleaseKeyGenerator;
import com.ctrip.framework.apollo.common.constants.GsonType;
import com.ctrip.framework.apollo.common.constants.ReleaseOperation;
import com.ctrip.framework.apollo.common.constants.ReleaseOperationContext;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.core.utils.StringUtils;

import org.apache.commons.lang.time.FastDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class ReleaseService {

  private static final FastDateFormat TIMESTAMP_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss");
  private Gson gson = new Gson();

  @Autowired
  private ReleaseRepository releaseRepository;
  @Autowired
  private ItemService itemService;
  @Autowired
  private AuditService auditService;
  @Autowired
  private NamespaceLockService namespaceLockService;
  @Autowired
  private NamespaceService namespaceService;
  @Autowired
  private NamespaceBranchService namespaceBranchService;
  @Autowired
  private ReleaseHistoryService releaseHistoryService;
  @Autowired
  private ItemSetService itemSetService;

  public Release findOne(long releaseId) {
    return releaseRepository.findById(releaseId).orElse(null);
  }


  public Release findActiveOne(long releaseId) {
    return releaseRepository.findByIdAndIsAbandonedFalse(releaseId);
  }

  public List<Release> findByReleaseIds(Set<Long> releaseIds) {
    Iterable<Release> releases = releaseRepository.findAllById(releaseIds);
    if (releases == null) {
      return Collections.emptyList();
    }
    return Lists.newArrayList(releases);
  }

  public List<Release> findByReleaseKeys(Set<String> releaseKeys) {
    return releaseRepository.findByReleaseKeyIn(releaseKeys);
  }

  public Release findLatestActiveRelease(Namespace namespace) {
    return findLatestActiveRelease(namespace.getAppId(),
                                   namespace.getClusterName(), namespace.getNamespaceName());

  }

  public Release findLatestActiveRelease(String appId, String clusterName, String namespaceName) {
    return releaseRepository.findFirstByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseOrderByIdDesc(appId,
                                                                                                            clusterName,
                                                                                                            namespaceName);
  }

  public List<Release> findAllReleases(String appId, String clusterName, String namespaceName, Pageable page) {
    List<Release> releases = releaseRepository.findByAppIdAndClusterNameAndNamespaceNameOrderByIdDesc(appId,
                                                                                                      clusterName,
                                                                                                      namespaceName,
                                                                                                      page);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  public List<Release> findActiveReleases(String appId, String clusterName, String namespaceName, Pageable page) {
    List<Release>
        releases =
        releaseRepository.findByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseOrderByIdDesc(appId, clusterName,
                                                                                                    namespaceName,
                                                                                                    page);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  @Transactional
  public Release mergeBranchChangeSetsAndRelease(Namespace namespace, String branchName, String releaseName,
                                                 String releaseComment, boolean isEmergencyPublish,
                                                 ItemChangeSets changeSets) {

    checkLock(namespace, isEmergencyPublish, changeSets.getDataChangeLastModifiedBy());

    itemSetService.updateSet(namespace, changeSets);

    Release branchRelease = findLatestActiveRelease(namespace.getAppId(), branchName, namespace
        .getNamespaceName());
    long branchReleaseId = branchRelease == null ? 0 : branchRelease.getId();

    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    Map<String, Object> operationContext = Maps.newHashMap();
    operationContext.put(ReleaseOperationContext.SOURCE_BRANCH, branchName);
    operationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, branchReleaseId);
    operationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);

    return masterRelease(namespace, releaseName, releaseComment, operateNamespaceItems,
                         changeSets.getDataChangeLastModifiedBy(),
                         ReleaseOperation.GRAY_RELEASE_MERGE_TO_MASTER, operationContext);

  }

  @Transactional
  public Release publish(Namespace namespace, String releaseName, String releaseComment,
                         String operator, boolean isEmergencyPublish) {

    // 校验锁定
    checkLock(namespace, isEmergencyPublish, operator);

    // 获得 Namespace 的普通配置 Map
    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    // 获得父 Namespace
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    // 若有父 Namespace ，则是子 Namespace ，进行灰度发布
    //branch release
    if (parentNamespace != null) {
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
                                    releaseName, releaseComment, operator, isEmergencyPublish);
    }

    // 获得子 Namespace 对象
    Namespace childNamespace = namespaceService.findChildNamespace(namespace);

    // 获得上一次，并且有效的 Release 对象
    Release previousRelease = null;
    if (childNamespace != null) {
      previousRelease = findLatestActiveRelease(namespace);
    }

    // 创建操作 Context
    //master release
    Map<String, Object> operationContext = Maps.newHashMap();
    operationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);

    // 主干发布
    Release release = masterRelease(namespace, releaseName, releaseComment, operateNamespaceItems,
                                    operator, ReleaseOperation.NORMAL_RELEASE, operationContext);

    // 若有子 Namespace 时，自动将主干合并到子 Namespace ，并进行一次子 Namespace 的发布
    //merge to branch and auto release
    if (childNamespace != null) {
      mergeFromMasterAndPublishBranch(namespace, childNamespace, operateNamespaceItems,
                                      releaseName, releaseComment, operator, previousRelease,
                                      release, isEmergencyPublish);
    }

    return release;
  }

  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {
    // 获得父 Namespace 的最后有效 Release 对象
    Release parentLatestRelease = findLatestActiveRelease(parentNamespace);
    // 获得父 Namespace 的配置项
    Map<String, String> parentConfigurations = parentLatestRelease != null ?
            gson.fromJson(parentLatestRelease.getConfigurations(),
                    GsonType.CONFIG) : new HashMap<>();
    // 获得父 Namespace 的 releaseId 属性
    long baseReleaseId = parentLatestRelease == null ? 0 : parentLatestRelease.getId();

    // 合并配置项
    Map<String, String> configsToPublish = mergeConfiguration(parentConfigurations, childNamespaceItems);

    if(!(grayDelKeys == null || grayDelKeys.size()==0)){
      for (String key : grayDelKeys){
        configsToPublish.remove(key);
      }
    }

    // 发布子 Namespace 的 Release
    return branchRelease(parentNamespace, childNamespace, releaseName, releaseComment,
            configsToPublish, baseReleaseId, operator,
            ReleaseOperation.GRAY_RELEASE, isEmergencyPublish);

  }

  @Transactional
  public Release grayDeletionPublish(Namespace namespace, String releaseName, String releaseComment,
                                     String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {

    checkLock(namespace, isEmergencyPublish, operator);

    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    //branch release
    if (parentNamespace != null) {
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
              releaseName, releaseComment, operator, isEmergencyPublish, grayDelKeys);
    }else {
      throw new NotFoundException("Parent namespace not found");
    }
  }

  private void checkLock(Namespace namespace, boolean isEmergencyPublish, String operator) {
    // 非紧急发布
    if (!isEmergencyPublish) {
      // 获得 NamespaceLock 对象
      NamespaceLock lock = namespaceLockService.findLock(namespace.getId());
      // 校验锁定人是否是当前管理员。若是，抛出 BadRequestException 异常
      if (lock != null && lock.getDataChangeCreatedBy().equals(operator)) {
        throw new BadRequestException("Config can not be published by yourself.");
      }
    }
  }

  /**
   * 在父 Namespace 发布 Release 后，
   * 会调用 #mergeFromMasterAndPublishBranch(...) 方法，
   * 自动将 父 Namespace (主干) 合并到子 Namespace (分支)，并进行一次子 Namespace 的发布。
   */
  private void mergeFromMasterAndPublishBranch(Namespace parentNamespace, Namespace childNamespace,
                                               Map<String, String> parentNamespaceItems,
                                               String releaseName, String releaseComment,
                                               String operator, Release masterPreviousRelease,
                                               Release parentRelease, boolean isEmergencyPublish) {
    // 获得子 Namespace 的配置 Map
    //create release for child namespace
    Map<String, String> childReleaseConfiguration = getNamespaceReleaseConfiguration(childNamespace);
    // 获得父 Namespace 的配置 Map
    Map<String, String> parentNamespaceOldConfiguration = masterPreviousRelease == null ?
                                                          null : gson.fromJson(masterPreviousRelease.getConfigurations(),
                                                                        GsonType.CONFIG);

    // 计算合并最新父 Namespace 的配置 Map 后的子 Namespace 的配置 Map
    Map<String, String> childNamespaceToPublishConfigs =
        calculateChildNamespaceToPublishConfiguration(parentNamespaceOldConfiguration,
                                                      parentNamespaceItems,
                                                      childNamespace);
    //compare
    // 若发生了变化，则进行一次子 Namespace 的发布
    if (!childNamespaceToPublishConfigs.equals(childReleaseConfiguration)) {
      branchRelease(parentNamespace, childNamespace, releaseName, releaseComment,
                    childNamespaceToPublishConfigs, parentRelease.getId(), operator,
                    ReleaseOperation.MASTER_NORMAL_RELEASE_MERGE_TO_GRAY, isEmergencyPublish);
    }

  }

  /**
   * 子 Namespace 发布 Release 。
   * 子 Namespace 会自动继承 父 Namespace 已经发布的配置。
   * 若有相同的配置项，使用 子 Namespace 的。
   * 配置处理的逻辑上，和关联 Namespace 是一致的。
   */
  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish) {
    return publishBranchNamespace(parentNamespace, childNamespace, childNamespaceItems, releaseName, releaseComment,
            operator, isEmergencyPublish, null);

  }

  private Release masterRelease(Namespace namespace, String releaseName, String releaseComment,
                                Map<String, String> configurations, String operator,
                                int releaseOperation, Map<String, Object> operationContext) {
    // 获得最后有效的 Release 对象
    Release lastActiveRelease = findLatestActiveRelease(namespace);
    long previousReleaseId = lastActiveRelease == null ? 0 : lastActiveRelease.getId();
    // 创建 Release 对象，并保存
    Release release = createRelease(namespace, releaseName, releaseComment,
                                    configurations, operator);

    // 创建 ReleaseHistory 对象，并保存
    releaseHistoryService.createReleaseHistory(namespace.getAppId(), namespace.getClusterName(),
                                               namespace.getNamespaceName(), namespace.getClusterName(),
                                               release.getId(), previousReleaseId, releaseOperation,
                                               operationContext, operator);

    return release;
  }

  private Release branchRelease(Namespace parentNamespace, Namespace childNamespace,
                                String releaseName, String releaseComment,
                                Map<String, String> configurations, long baseReleaseId,
                                String operator, int releaseOperation, boolean isEmergencyPublish) {
    // 获得父 Namespace 最后有效的 Release 对象
    Release previousRelease = findLatestActiveRelease(childNamespace.getAppId(),
                                                      childNamespace.getClusterName(),
                                                      childNamespace.getNamespaceName());
    // 获得父 Namespace 最后有效的 Release 对象的编号
    long previousReleaseId = previousRelease == null ? 0 : previousRelease.getId();

    // 创建 Map ，用于 ReleaseHistory 对象的 `operationContext` 属性。
    Map<String, Object> releaseOperationContext = Maps.newHashMap();
    releaseOperationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, baseReleaseId);
    releaseOperationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);

    // 创建子 Namespace 的 Release 对象，并保存
    Release release =
        createRelease(childNamespace, releaseName, releaseComment, configurations, operator);

    // 更新 GrayReleaseRule 的 releaseId 属性
    //update gray release rules
    GrayReleaseRule grayReleaseRule = namespaceBranchService.updateRulesReleaseId(childNamespace.getAppId(),
                                                                                  parentNamespace.getClusterName(),
                                                                                  childNamespace.getNamespaceName(),
                                                                                  childNamespace.getClusterName(),
                                                                                  release.getId(), operator);

    // 创建 ReleaseHistory 对象，并保存
    if (grayReleaseRule != null) {
      releaseOperationContext.put(ReleaseOperationContext.RULES, GrayReleaseRuleItemTransformer
          .batchTransformFromJSON(grayReleaseRule.getRules()));
    }

    releaseHistoryService.createReleaseHistory(parentNamespace.getAppId(), parentNamespace.getClusterName(),
                                               parentNamespace.getNamespaceName(), childNamespace.getClusterName(),
                                               release.getId(),
                                               previousReleaseId, releaseOperation, releaseOperationContext, operator);

    return release;
  }

  private Map<String, String> mergeConfiguration(Map<String, String> baseConfigurations,
                                                 Map<String, String> coverConfigurations) {
    Map<String, String> result = new HashMap<>();
    //copy base configuration
    // 父 Namespace 的配置项
    for (Map.Entry<String, String> entry : baseConfigurations.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }

    //update and publish
    // 子 Namespace 的配置项
    for (Map.Entry<String, String> entry : coverConfigurations.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }

    // 返回合并后的配置项
    return result;
  }


  private Map<String, String> getNamespaceItems(Namespace namespace) {
    // 读取 Namespace 的 Item 集合
    List<Item> items = itemService.findItemsWithoutOrdered(namespace.getId());
    // 生成普通配置 Map 。过滤掉注释和空行的配置项
    Map<String, String> configurations = new HashMap<String, String>();
    for (Item item : items) {
      if (StringUtils.isEmpty(item.getKey())) {
        continue;
      }
      configurations.put(item.getKey(), item.getValue());
    }

    return configurations;
  }

  private Map<String, String> getNamespaceReleaseConfiguration(Namespace namespace) {
    // 获得最后有效的 Release 对象
    Release release = findLatestActiveRelease(namespace);
    Map<String, String> configuration = new HashMap<>();
    // 获得配置 Map
    if (release != null) {
      configuration = new Gson().fromJson(release.getConfigurations(), GsonType.CONFIG);
    }
    return configuration;
  }

  private Release createRelease(Namespace namespace, String name, String comment,
                                Map<String, String> configurations, String operator) {
    // 创建 Release 对象
    Release release = new Release();
    release.setReleaseKey(ReleaseKeyGenerator.generateReleaseKey(namespace));
    release.setDataChangeCreatedTime(new Date());
    release.setDataChangeCreatedBy(operator);
    release.setDataChangeLastModifiedBy(operator);
    release.setName(name);
    release.setComment(comment);
    release.setAppId(namespace.getAppId());
    release.setClusterName(namespace.getClusterName());
    release.setNamespaceName(namespace.getNamespaceName());
    // 使用 Gson ，将配置 Map 格式化成字符串。
    release.setConfigurations(gson.toJson(configurations));
    // 保存 Release 对象
    release = releaseRepository.save(release);

    // 释放 NamespaceLock
    namespaceLockService.unlock(namespace.getId());
    // 记录 Audit 到数据库中
    auditService.audit(Release.class.getSimpleName(), release.getId(), Audit.OP.INSERT,
                       release.getDataChangeCreatedBy());

    return release;
  }

  @Transactional
  public Release rollback(long releaseId, String operator) {
    Release release = findOne(releaseId);
    if (release == null) {
      throw new NotFoundException("release not found");
    }
    if (release.isAbandoned()) {
      throw new BadRequestException("release is not active");
    }

    String appId = release.getAppId();
    String clusterName = release.getClusterName();
    String namespaceName = release.getNamespaceName();

    PageRequest page = PageRequest.of(0, 2);
    List<Release> twoLatestActiveReleases = findActiveReleases(appId, clusterName, namespaceName, page);
    if (twoLatestActiveReleases == null || twoLatestActiveReleases.size() < 2) {
      throw new BadRequestException(String.format(
          "Can't rollback namespace(appId=%s, clusterName=%s, namespaceName=%s) because there is only one active release",
          appId,
          clusterName,
          namespaceName));
    }

    release.setAbandoned(true);
    release.setDataChangeLastModifiedBy(operator);

    releaseRepository.save(release);

    releaseHistoryService.createReleaseHistory(appId, clusterName,
                                               namespaceName, clusterName, twoLatestActiveReleases.get(1).getId(),
                                               release.getId(), ReleaseOperation.ROLLBACK, null, operator);

    //publish child namespace if namespace has child
    rollbackChildNamespace(appId, clusterName, namespaceName, twoLatestActiveReleases, operator);

    return release;
  }

  private void rollbackChildNamespace(String appId, String clusterName, String namespaceName,
                                      List<Release> parentNamespaceTwoLatestActiveRelease, String operator) {
    Namespace parentNamespace = namespaceService.findOne(appId, clusterName, namespaceName);
    Namespace childNamespace = namespaceService.findChildNamespace(appId, clusterName, namespaceName);
    if (parentNamespace == null || childNamespace == null) {
      return;
    }

    Release abandonedRelease = parentNamespaceTwoLatestActiveRelease.get(0);
    Release parentNamespaceNewLatestRelease = parentNamespaceTwoLatestActiveRelease.get(1);

    Map<String, String> parentNamespaceAbandonedConfiguration = gson.fromJson(abandonedRelease.getConfigurations(),
                                                                              GsonType.CONFIG);

    Map<String, String>
        parentNamespaceNewLatestConfiguration =
        gson.fromJson(parentNamespaceNewLatestRelease.getConfigurations(), GsonType.CONFIG);

    Map<String, String>
        childNamespaceNewConfiguration =
        calculateChildNamespaceToPublishConfiguration(parentNamespaceAbandonedConfiguration,
                                                      parentNamespaceNewLatestConfiguration,
                                                      childNamespace);

    branchRelease(parentNamespace, childNamespace,
                  TIMESTAMP_FORMAT.format(new Date()) + "-master-rollback-merge-to-gray", "",
                  childNamespaceNewConfiguration, parentNamespaceNewLatestRelease.getId(), operator,
                  ReleaseOperation.MATER_ROLLBACK_MERGE_TO_GRAY, false);
  }

  private Map<String, String> calculateChildNamespaceToPublishConfiguration(
      Map<String, String> parentNamespaceOldConfiguration,
      Map<String, String> parentNamespaceNewConfiguration,
      Namespace childNamespace) {
    // 获得子 Namespace 的最后有效的 Release 对象
    //first. calculate child namespace modified configs
    Release childNamespaceLatestActiveRelease = findLatestActiveRelease(childNamespace);

    // 获得子 Namespace 的配置 Map
    Map<String, String> childNamespaceLatestActiveConfiguration = childNamespaceLatestActiveRelease == null ? null :
                                                                  gson.fromJson(childNamespaceLatestActiveRelease
                                                                                    .getConfigurations(),
                                                                                GsonType.CONFIG);

    // 以子 Namespace 的配置 Map 为基础，计算出差异的 Map
    Map<String, String> childNamespaceModifiedConfiguration = calculateBranchModifiedItemsAccordingToRelease(
        parentNamespaceOldConfiguration, childNamespaceLatestActiveConfiguration);

    //second. append child namespace modified configs to parent namespace new latest configuration
    return mergeConfiguration(parentNamespaceNewConfiguration, childNamespaceModifiedConfiguration);
  }

  private Map<String, String> calculateBranchModifiedItemsAccordingToRelease(
      Map<String, String> masterReleaseConfigs,
      Map<String, String> branchReleaseConfigs) {

    // 差异 Map
    Map<String, String> modifiedConfigs = new HashMap<>();

    // 若子 Namespace 的配置 Map 为空，直接返回空 Map
    if (CollectionUtils.isEmpty(branchReleaseConfigs)) {
      return modifiedConfigs;
    }

    // 若父 Namespace 的配置 Map 为空，直接返回子 Namespace 的配置 Map
    if (CollectionUtils.isEmpty(masterReleaseConfigs)) {
      return branchReleaseConfigs;
    }

    // 以子 Namespace 的配置 Map 为基础，计算出差异的 Map
    for (Map.Entry<String, String> entry : branchReleaseConfigs.entrySet()) {

      if (!Objects.equals(entry.getValue(), masterReleaseConfigs.get(entry.getKey()))) {
        modifiedConfigs.put(entry.getKey(), entry.getValue());
      }
    }

    return modifiedConfigs;

  }

  @Transactional
  public int batchDelete(String appId, String clusterName, String namespaceName, String operator) {
    return releaseRepository.batchDelete(appId, clusterName, namespaceName, operator);
  }

}
