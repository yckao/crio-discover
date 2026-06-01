// Package discover discovers running CRI-O containers through the
// Kubernetes CRI runtime service on a local unix socket.
//
// It provides one-time listing and continuous watching APIs. Continuous
// watching uses polling as the reliability baseline and can use CRI container
// events to reduce notification latency when the runtime supports them.
package discover
