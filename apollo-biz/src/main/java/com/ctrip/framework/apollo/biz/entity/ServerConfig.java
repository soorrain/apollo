package com.ctrip.framework.apollo.biz.entity;

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
@Table(name = "ServerConfig")
@SQLDelete(sql = "Update ServerConfig set isDeleted = 1 where id = ?")
@Where(clause = "isDeleted = 0")
public class ServerConfig extends BaseEntity {
  /**
   * KEY
   */
  @Column(name = "Key", nullable = false)
  private String key;

  /**
   * Cluster 名
   */
  @Column(name = "Cluster", nullable = false)
  private String cluster;

  /**
   * VALUE
   */
  @Column(name = "Value", nullable = false)
  private String value;

  /**
   * 备注
   */
  @Column(name = "Comment", nullable = false)
  private String comment;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String toString() {
    return toStringHelper().add("key", key).add("value", value).add("comment", comment).toString();
  }
}
