/*
 Navicat Premium Data Transfer

 Source Server         : 本地-3306
 Source Server Type    : MySQL
 Source Server Version : 50730
 Source Host           : localhost:3306
 Source Schema         : test

 Target Server Type    : MySQL
 Target Server Version : 50730
 File Encoding         : 65001

 Date: 24/09/2020 11:32:39
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for students
-- ----------------------------
DROP TABLE IF EXISTS `students`;
CREATE TABLE `students`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `age` varchar(11) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `gender` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `created` timestamp(0) NULL DEFAULT NULL,
  `modified` timestamp(0) NULL DEFAULT NULL,
  `class_id` int(11) NULL DEFAULT NULL,
  `vender_id` int(11) NULL DEFAULT NULL,
  `dt` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_dt`(`dt`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of students
-- ----------------------------
INSERT INTO `students` VALUES (1, 'wangwei', '20', 'man', '2020-05-14 15:33:46', '2020-05-14 15:33:46', 13602, 111, '20200803');
INSERT INTO `students` VALUES (2, 'hhhhh', '40', 'man', '2020-05-14 15:33:46', '2020-05-14 15:33:46', 8601, 111, '20200804');

SET FOREIGN_KEY_CHECKS = 1;
