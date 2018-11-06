package com.ctrip.framework.apollo.adminservice.controller;

import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.service.AppNamespaceService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.common.dto.AppNamespaceDTO;
import com.ctrip.framework.apollo.common.dto.NamespaceDTO;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.core.utils.StringUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class AppNamespaceController {

  @Autowired
  private AppNamespaceService appNamespaceService;
  @Autowired
  private NamespaceService namespaceService;

  /**
   * 创建 AppNamespace
   * @param appNamespace AppNamespaceDTO 对象
   * @return AppNamespaceDTO 对象
   */
  @RequestMapping(value = "/apps/{appId}/appnamespaces", method = RequestMethod.POST)
  public AppNamespaceDTO create(@RequestBody AppNamespaceDTO appNamespace) {

    // 将 AppNamespaceDTO 转换成 AppNamespace 对象
    AppNamespace entity = BeanUtils.transfrom(AppNamespace.class, appNamespace);
    // 判断 `name` 在 App 下是否已经存在对应的 AppNamespace 对象。若已经存在，抛出 BadRequestException 异常。
    AppNamespace managedEntity = appNamespaceService.findOne(entity.getAppId(), entity.getName());

    if (managedEntity != null) {
      throw new BadRequestException("app namespaces already exist.");
    }

    // 设置 AppNamespace 的 `format` 属性为 "properties"，若为null。
    if (StringUtils.isEmpty(entity.getFormat())){
      entity.setFormat(ConfigFileFormat.Properties.getValue());
    }

    // 保存 AppNamespace 对象到数据库
    entity = appNamespaceService.createAppNamespace(entity);

    // 将保存的 AppNamespace 对象转化成 AppNamespaceDTO 返回
    return BeanUtils.transfrom(AppNamespaceDTO.class, entity);
  }

  @RequestMapping(value = "/apps/{appId}/appnamespaces/{namespaceName:.+}", method = RequestMethod.DELETE)
  public void delete(@PathVariable("appId") String appId, @PathVariable("namespaceName") String namespaceName,
      @RequestParam String operator) {
    AppNamespace entity = appNamespaceService.findOne(appId, namespaceName);
    if (entity == null) {
      throw new BadRequestException("app namespace not found for appId: " + appId + " namespace: " + namespaceName);
    }
    appNamespaceService.deleteAppNamespace(entity, operator);
  }

  @RequestMapping(value = "/appnamespaces/{publicNamespaceName}/namespaces", method = RequestMethod.GET)
  public List<NamespaceDTO> findPublicAppNamespaceAllNamespaces(@PathVariable String publicNamespaceName, Pageable pageable) {

    List<Namespace> namespaces = namespaceService.findPublicAppNamespaceAllNamespaces(publicNamespaceName, pageable);

    return BeanUtils.batchTransform(NamespaceDTO.class, namespaces);
  }

  @RequestMapping(value = "/appnamespaces/{publicNamespaceName}/associated-namespaces/count", method = RequestMethod.GET)
  public int countPublicAppNamespaceAssociatedNamespaces(@PathVariable String publicNamespaceName) {
    return namespaceService.countPublicAppNamespaceAssociatedNamespaces(publicNamespaceName);
  }

}
