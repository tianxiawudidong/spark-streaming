/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.gm;

/**
 *
 * @author root
 */
public interface GearmanResult {

    boolean parseResult();

    boolean packSendMsg();
}
