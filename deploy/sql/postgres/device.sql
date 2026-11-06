/*
 Navicat Premium Data Transfer

 Source Server         : postgres
 Source Server Type    : PostgreSQL
 Source Server Version : 180003
 Source Host           : localhost:5432
 Source Catalog        : postgres
 Source Schema         : public

 Target Server Type    : PostgreSQL
 Target Server Version : 180003
 File Encoding         : 65001

 Date: 06/11/2026 14:26:46
*/


-- ----------------------------
-- Table structure for device
-- ----------------------------
DROP TABLE IF EXISTS "public"."device";
CREATE TABLE "public"."device" (
  "id" int8 NOT NULL DEFAULT nextval('device_id_seq'::regclass),
  "sn" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "cer" varchar(255) COLLATE "pg_catalog"."default",
  "key" varchar(255) COLLATE "pg_catalog"."default"
)
;

-- ----------------------------
-- Uniques structure for table device
-- ----------------------------
ALTER TABLE "public"."device" ADD CONSTRAINT "idx_sn" UNIQUE ("sn");

-- ----------------------------
-- Primary Key structure for table device
-- ----------------------------
ALTER TABLE "public"."device" ADD CONSTRAINT "device_pkey" PRIMARY KEY ("id");
