#include "tcp_receiver.hh"

// Dummy implementation of a TCP receiver

// For Lab 2, please replace with a real implementation that passes the
// automated checks run by `make check_lab2`.

template <typename... Targs>
void DUMMY_CODE(Targs &&... /* unused */) {}

using namespace std;

 void TCPReceiver::segment_received(const TCPSegment &seg) {
    const TCPHeader &_tcp_header = seg.header();
     /*
      * 不需要考虑很多,直接按照tcp接收端状态转换图写就行,其他情况都可以交给reassembler处理
      */
     if (!_syn)  // 如果未收到过syn
     {
         if (!_tcp_header.syn)// 如果包中含有syn就继续,没有就直接返回,抛弃包
             return;
         _syn = true;
         _isn = _tcp_header.seqno;
     }
     // ack 期望下一个收到的片段索引
     uint64_t _ackno = _reassembler.stream_out().bytes_written() + 1;
     // 计算 seq
     uint64_t _seqno = unwrap(_tcp_header.seqno, _isn, _ackno);
     // 注意syn也占用seqno,所以别忘了
     uint64_t _index = _seqno - 1 + static_cast<uint64_t>(_tcp_header.syn);
     _reassembler.push_substring(seg.payload().copy(), _index, _tcp_header.fin);
 }
 optional<WrappingInt32> TCPReceiver::ackno() const {

     if (!_syn) // 如果未建立连接,直接返回空
         return nullopt;
     uint64_t _ackno = _reassembler.stream_out().bytes_written() + 1;
     if (_reassembler.stream_out().input_ended())// 如果结束连接,返回的ack要算上sender发来的fin
         _ackno++;
     return WrappingInt32(_isn) + _ackno;
 }
 // 如下
 size_t TCPReceiver::window_size() const { return _capacity - _reassembler.stream_out().buffer_size(); }
 