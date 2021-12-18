package com.github.tomboyo.silverbroccoli.auditlog;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface AuditLogRepository extends CrudRepository<AuditLog, Long> {

  /**
   * Create a new audit log entry if and only if another with the same message does not already
   * exist. This is idempotent.
   */
  @Transactional
  @Modifying
  @Query(
      nativeQuery = true,
      value = "insert into audit_log (message) values (?1) on conflict (message) do nothing")
  void createIfNotExists(String message);
}
