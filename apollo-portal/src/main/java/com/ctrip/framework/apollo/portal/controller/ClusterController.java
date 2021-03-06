package com.ctrip.framework.apollo.portal.controller;

import com.ctrip.framework.apollo.common.dto.ClusterDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.InputValidator;
import com.ctrip.framework.apollo.common.utils.RequestPrecondition;
import com.ctrip.framework.apollo.core.enums.Env;
import com.ctrip.framework.apollo.portal.service.ClusterService;
import com.ctrip.framework.apollo.portal.spi.UserInfoHolder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

import static com.ctrip.framework.apollo.common.utils.RequestPrecondition.checkModel;

@RestController
public class ClusterController {

  @Autowired
  private ClusterService clusterService;
  @Autowired
  private UserInfoHolder userInfoHolder;

  @PreAuthorize(value = "@permissionValidator.hasCreateClusterPermission(#appId)")
  @RequestMapping(value = "apps/{appId}/envs/{env}/clusters", method = RequestMethod.POST)
  public ClusterDTO createCluster(@PathVariable String appId, @PathVariable String env,
                                  @RequestBody ClusterDTO cluster) {

    // 校验 ClusterDTO 非空
    checkModel(Objects.nonNull(cluster));
    // 校验 ClusterDTO 的 `appId` 和 `name` 非空
    RequestPrecondition.checkArgumentsNotEmpty(cluster.getAppId(), cluster.getName());

    // 校验 ClusterDTO 的 `name` 格式正确
    if (!InputValidator.isValidClusterNamespace(cluster.getName())) {
      throw new BadRequestException(String.format("Cluster格式错误: %s", InputValidator.INVALID_CLUSTER_NAMESPACE_MESSAGE));
    }

    // 设置 ClusterDTO 的创建和修改人为当前登陆用户
    String operator = userInfoHolder.getUser().getUserId();
    cluster.setDataChangeLastModifiedBy(operator);
    cluster.setDataChangeCreatedBy(operator);

    // 创建 Cluster 到 Admin Service
    return clusterService.createCluster(Env.valueOf(env), cluster);
  }

  @PreAuthorize(value = "@permissionValidator.isSuperAdmin()")
  @RequestMapping(value = "apps/{appId}/envs/{env}/clusters/{clusterName:.+}", method = RequestMethod.DELETE)
  public ResponseEntity<Void> deleteCluster(@PathVariable String appId, @PathVariable String env,
                                            @PathVariable String clusterName){
    clusterService.deleteCluster(Env.fromString(env), appId, clusterName);
    return ResponseEntity.ok().build();
  }

  @RequestMapping(value = "apps/{appId}/envs/{env}/clusters/{clusterName:.+}", method = RequestMethod.GET)
  public ClusterDTO loadCluster(@PathVariable("appId") String appId, @PathVariable String env, @PathVariable("clusterName") String clusterName) {

    return clusterService.loadCluster(appId, Env.fromString(env), clusterName);
  }

}
