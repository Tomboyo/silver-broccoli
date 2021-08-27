package com.github.tomboyo.silverbroccoli;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import static javax.persistence.GenerationType.IDENTITY;

@Entity
@Table(name = "audit_log")
public class AuditLog {

  @Id
  @GeneratedValue(strategy = IDENTITY)
  private Long id;
  
  @Column(unique = true)
  private String message;

  public Long getId() {
    return id;
  }

  public AuditLog id(Long id) {
    this.id = id;
    return this;
  }

  public String getMessage() {
    return message;
  }

  public AuditLog message(String message) {
    this.message = message;
    return this;
  }

  @Override
  public String toString() {
    return "AuditLog{" +
        "id=" + id +
        ", message='" + message + '\'' +
        '}';
  }
}
