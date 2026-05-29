package util

import (
	"log"
	"net"
	"os"
	"strconv"

	"github.com/zeromicro/go-zero/rest"
)

func GetRegistryParameters(c rest.RestConf) (serviceName, ip, portStr string) {
	serviceName = os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = c.Name
	}

	ip = os.Getenv("SERVICE_IP")
	if ip == "" {
		ip = getLocalIP()
		if ip == "" {
			ip = "127.0.0.1"
			log.Println("[WARN] Failed to detect local IP, fallback to 127.0.0.1")
		} else {
			log.Printf("[INFO] Auto-detected local IP: %s", ip)
		}
	} else {
		log.Printf("[INFO] Using SERVICE_IP from env: %s", ip)
	}

	portStr = os.Getenv("SERVICE_PORT")
	if portStr == "" {
		portStr = strconv.Itoa(c.Port)
	}
	return
}

func getLocalIP() string {
	// 方法1：通过一个外部地址获取本机出口 IP
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		ip := localAddr.IP.String()
		if ip != "" && ip != "::1" && !net.IP.IsLoopback(localAddr.IP) {
			return ip
		}
	}

	// 方法2：遍历网卡
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			// 过滤 docker 等虚拟网卡可选择增加判断，这里简化，取第一个有效 IPv4
			return ipnet.IP.String()
		}
	}
	return ""
}
