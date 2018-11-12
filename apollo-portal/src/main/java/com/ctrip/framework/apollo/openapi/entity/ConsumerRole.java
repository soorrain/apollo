package com.ctrip.framework.apollo.openapi.entity;

import com.ctrip.framework.apollo.common.entity.BaseEntity;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.Where;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Entity
@Table(name = "ConsumerRole")
@SQLDelete(sql = "Update ConsumerRole set isDeleted = 1 where id = ?")
@Where(clause = "isDeleted = 0")
public class ConsumerRole extends BaseEntity {
  /**
   * Consumer 编号 {@link Consumer#id}
   */
  @Column(name = "ConsumerId", nullable = false)
  private long consumerId;

  /**
   * Role 编号 {@link com.ctrip.framework.apollo.portal.entity.po.Role#id}
   */
  @Column(name = "RoleId", nullable = false)
  private long roleId;

  public long getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(long consumerId) {
    this.consumerId = consumerId;
  }

  public long getRoleId() {
    return roleId;
  }

  public void setRoleId(long roleId) {
    this.roleId = roleId;
  }

  @Override
  public String toString() {
    return toStringHelper().add("consumerId", consumerId).add("roleId", roleId).toString();
  }
}
