package com.ctrip.framework.apollo.portal.spi.defaultimpl;

import com.ctrip.framework.apollo.openapi.repository.ConsumerRoleRepository;
import com.ctrip.framework.apollo.portal.component.config.PortalConfig;
import com.ctrip.framework.apollo.portal.entity.bo.UserInfo;
import com.ctrip.framework.apollo.portal.entity.po.Permission;
import com.ctrip.framework.apollo.portal.entity.po.Role;
import com.ctrip.framework.apollo.portal.entity.po.RolePermission;
import com.ctrip.framework.apollo.portal.entity.po.UserRole;
import com.ctrip.framework.apollo.portal.repository.PermissionRepository;
import com.ctrip.framework.apollo.portal.repository.RolePermissionRepository;
import com.ctrip.framework.apollo.portal.repository.RoleRepository;
import com.ctrip.framework.apollo.portal.repository.UserRoleRepository;
import com.ctrip.framework.apollo.portal.service.RolePermissionService;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

/**
 * Created by timothy on 2017/4/26.
 */
public class DefaultRolePermissionService implements RolePermissionService {
    @Autowired
    private RoleRepository roleRepository;
    @Autowired
    private RolePermissionRepository rolePermissionRepository;
    @Autowired
    private UserRoleRepository userRoleRepository;
    @Autowired
    private PermissionRepository permissionRepository;
    @Autowired
    private PortalConfig portalConfig;
    @Autowired
    private ConsumerRoleRepository consumerRoleRepository;

    /**
     * Create role with permissions, note that role name should be unique
     */
    @Transactional
    public Role createRoleWithPermissions(Role role, Set<Long> permissionIds) {
        // 获得 Role 对象，校验 Role 不存在
        Role current = findRoleByRoleName(role.getRoleName());
        Preconditions.checkState(current == null, "Role %s already exists!", role.getRoleName());

        // 新增 Role
        Role createdRole = roleRepository.save(role);

        // 授权给 Role
        if (!CollectionUtils.isEmpty(permissionIds)) {
            // 创建 RolePermission 数组
            Iterable<RolePermission> rolePermissions = FluentIterable.from(permissionIds).transform(
                    permissionId -> {
                        RolePermission rolePermission = new RolePermission();
                        rolePermission.setRoleId(createdRole.getId());
                        rolePermission.setPermissionId(permissionId);
                        rolePermission.setDataChangeCreatedBy(createdRole.getDataChangeCreatedBy());
                        rolePermission.setDataChangeLastModifiedBy(createdRole.getDataChangeLastModifiedBy());
                        return rolePermission;
                    });
            // 保存 RolePermission 数组
            rolePermissionRepository.saveAll(rolePermissions);
        }

        return createdRole;
    }

    /**
     * Assign role to users
     *
     * @return the users assigned roles
     */
    @Transactional
    public Set<String> assignRoleToUsers(String roleName, Set<String> userIds,
                                         String operatorUserId) {
        // 获得 Role 对象，校验 Role 存在
        Role role = findRoleByRoleName(roleName);
        Preconditions.checkState(role != null, "Role %s doesn't exist!", roleName);

        // 获得已存在的 UserRole 数组
        List<UserRole> existedUserRoles =
                userRoleRepository.findByUserIdInAndRoleId(userIds, role.getId());
        // 排除已经存在的
        Set<String> existedUserIds =
                FluentIterable.from(existedUserRoles).transform(userRole -> userRole.getUserId()).toSet();

        Set<String> toAssignUserIds = Sets.difference(userIds, existedUserIds);

        // 创建需要新增的 UserRole 数组
        Iterable<UserRole> toCreate = FluentIterable.from(toAssignUserIds).transform(userId -> {
            UserRole userRole = new UserRole();
            userRole.setRoleId(role.getId());
            userRole.setUserId(userId);
            userRole.setDataChangeCreatedBy(operatorUserId);
            userRole.setDataChangeLastModifiedBy(operatorUserId);
            return userRole;
        });

        // 保存 RolePermission 数组
        userRoleRepository.saveAll(toCreate);
        return toAssignUserIds;
    }

    /**
     * Remove role from users
     */
    @Transactional
    public void removeRoleFromUsers(String roleName, Set<String> userIds, String operatorUserId) {
        // 获得 Role 对象，校验 Role 存在
        Role role = findRoleByRoleName(roleName);
        Preconditions.checkState(role != null, "Role %s doesn't exist!", roleName);

        // 获得已存在的 UserRole 数组
        List<UserRole> existedUserRoles =
                userRoleRepository.findByUserIdInAndRoleId(userIds, role.getId());

        // 标记删除
        for (UserRole userRole : existedUserRoles) {
            userRole.setDeleted(true);
            userRole.setDataChangeLastModifiedTime(new Date());
            userRole.setDataChangeLastModifiedBy(operatorUserId);
        }

        // 保存 RolePermission 数组 【标记删除】
        userRoleRepository.saveAll(existedUserRoles);
    }

    /**
     * Query users with role
     */
    public Set<UserInfo> queryUsersWithRole(String roleName) {
        // 获得 Role 对象，校验 Role 存在
        Role role = findRoleByRoleName(roleName);

        // Role 不存在时，返回空数组
        if (role == null) {
            return Collections.emptySet();
        }

        // 获得 UserRole 数组
        List<UserRole> userRoles = userRoleRepository.findByRoleId(role.getId());

        // 转换成 UserInfo 数组
        Set<UserInfo> users = FluentIterable.from(userRoles).transform(userRole -> {
            UserInfo userInfo = new UserInfo();
            userInfo.setUserId(userRole.getUserId());
            return userInfo;
        }).toSet();

        return users;
    }

    /**
     * Find role by role name, note that roleName should be unique
     */
    public Role findRoleByRoleName(String roleName) {
        return roleRepository.findTopByRoleName(roleName);
    }

    /**
     * Check whether user has the permission
     */
    public boolean userHasPermission(String userId, String permissionType, String targetId) {
        // 获得 Permission 对象
        Permission permission =
                permissionRepository.findTopByPermissionTypeAndTargetId(permissionType, targetId);
        // 若 Permission 不存在，返回 false
        if (permission == null) {
            return false;
        }

        // 若是超级管理员，返回 true 【有权限】
        if (isSuperAdmin(userId)) {
            return true;
        }

        // 获得 UserRole 数组
        List<UserRole> userRoles = userRoleRepository.findByUserId(userId);
        // 若数组为空，返回 false
        if (CollectionUtils.isEmpty(userRoles)) {
            return false;
        }

        // 获得 RolePermission 数组
        Set<Long> roleIds =
                FluentIterable.from(userRoles).transform(userRole -> userRole.getRoleId()).toSet();
        List<RolePermission> rolePermissions = rolePermissionRepository.findByRoleIdIn(roleIds);
        // 若数组为空，返回 false
        if (CollectionUtils.isEmpty(rolePermissions)) {
            return false;
        }

        // 判断是否有对应的 RolePermission 。若有，则返回 true 【有权限】
        for (RolePermission rolePermission : rolePermissions) {
            if (rolePermission.getPermissionId() == permission.getId()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<Role> findUserRoles(String userId) {
        List<UserRole> userRoles = userRoleRepository.findByUserId(userId);
        if (CollectionUtils.isEmpty(userRoles)) {
            return Collections.emptyList();
        }

        Set<Long> roleIds = userRoles.stream().map(UserRole::getRoleId).collect(Collectors.toSet());

        return Lists.newLinkedList(roleRepository.findAllById(roleIds));
    }

    public boolean isSuperAdmin(String userId) {
        return portalConfig.superAdmins().contains(userId);
    }

    /**
     * Create permission, note that permissionType + targetId should be unique
     */
    @Transactional
    public Permission createPermission(Permission permission) {
        String permissionType = permission.getPermissionType();
        String targetId = permission.getTargetId();
        // 获得 Permission 对象，校验 Permission 为空
        Permission current =
                permissionRepository.findTopByPermissionTypeAndTargetId(permissionType, targetId);
        Preconditions.checkState(current == null,
                "Permission with permissionType %s targetId %s already exists!", permissionType, targetId);

        // 保存 Permission
        return permissionRepository.save(permission);
    }

    /**
     * Create permissions, note that permissionType + targetId should be unique
     */
    @Transactional
    public Set<Permission> createPermissions(Set<Permission> permissions) {
        // 创建 Multimap 对象，用于下面校验的分批的批量查询
        Multimap<String, String> targetIdPermissionTypes = HashMultimap.create();
        for (Permission permission : permissions) {
            targetIdPermissionTypes.put(permission.getTargetId(), permission.getPermissionType());
        }

        // 查询 Permission 集合，校验都不存在
        for (String targetId : targetIdPermissionTypes.keySet()) {
            Collection<String> permissionTypes = targetIdPermissionTypes.get(targetId);
            List<Permission> current =
                    permissionRepository.findByPermissionTypeInAndTargetId(permissionTypes, targetId);
            Preconditions.checkState(CollectionUtils.isEmpty(current),
                    "Permission with permissionType %s targetId %s already exists!", permissionTypes,
                    targetId);
        }

        // 保存 Permission 集合
        Iterable<Permission> results = permissionRepository.saveAll(permissions);
        // 转成 Permission 集合，返回
        return FluentIterable.from(results).toSet();
    }

    @Transactional
    @Override
    public void deleteRolePermissionsByAppId(String appId, String operator) {
        List<Long> permissionIds = permissionRepository.findPermissionIdsByAppId(appId);

        if (!permissionIds.isEmpty()) {
            // 1. delete Permission
            permissionRepository.batchDelete(permissionIds, operator);

            // 2. delete Role Permission
            rolePermissionRepository.batchDeleteByPermissionIds(permissionIds, operator);
        }

        List<Long> roleIds = roleRepository.findRoleIdsByAppId(appId);

        if (!roleIds.isEmpty()) {
            // 3. delete Role
            roleRepository.batchDelete(roleIds, operator);

            // 4. delete User Role
            userRoleRepository.batchDeleteByRoleIds(roleIds, operator);

            // 5. delete Consumer Role
            consumerRoleRepository.batchDeleteByRoleIds(roleIds, operator);
        }
    }

    @Transactional
    @Override
    public void deleteRolePermissionsByAppIdAndNamespace(String appId, String namespaceName, String operator) {
        List<Long> permissionIds = permissionRepository.findPermissionIdsByAppIdAndNamespace(appId, namespaceName);

        if (!permissionIds.isEmpty()) {
            // 1. delete Permission
            permissionRepository.batchDelete(permissionIds, operator);

            // 2. delete Role Permission
            rolePermissionRepository.batchDeleteByPermissionIds(permissionIds, operator);
        }

        List<Long> roleIds = roleRepository.findRoleIdsByAppIdAndNamespace(appId, namespaceName);

        if (!roleIds.isEmpty()) {
            // 3. delete Role
            roleRepository.batchDelete(roleIds, operator);

            // 4. delete User Role
            userRoleRepository.batchDeleteByRoleIds(roleIds, operator);

            // 5. delete Consumer Role
            consumerRoleRepository.batchDeleteByRoleIds(roleIds, operator);
        }
    }
}
