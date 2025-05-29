
CREATE DATABASE `alphabot`;

USE `alphabot`;


CREATE TABLE `tb_bot_approve` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_addr` varchar(256) NOT NULL DEFAULT '',
  `token_addr` varchar(256) NOT NULL DEFAULT '',
  `approve_amount` varchar(256) NOT NULL DEFAULT '',
  `create_time` datetime NOT NULL,
  `update_time` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_idx_user_token` (`user_addr`,`token_addr`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `tb_bot_wallet` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `wallet_addr` varchar(256) NOT NULL DEFAULT '',
  `private_key` varchar(256) NOT NULL DEFAULT '',
  `using` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `tb_contract_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `token_name` varchar(64) NOT NULL DEFAULT '',
  `contract_addr` varchar(256) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_idx_contract_addr` (`contract_addr`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `tb_okx_api_key` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `api_key` varchar(256) NOT NULL DEFAULT '',
  `secret_key` varchar(256) NOT NULL DEFAULT '',
  `token` varchar(256) NOT NULL DEFAULT '' COMMENT 'which token use',
  `passphrase` varchar(256) NOT NULL DEFAULT '',
  `bot_id` int(11) NOT NULL DEFAULT '0' COMMENT 'which bot use',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4;
