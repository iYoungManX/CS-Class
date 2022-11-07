#include "stream_reassembler.hh"

// Dummy implementation of a stream reassembler.

// For Lab 1, please replace with a real implementation that passes the
// automated checks run by `make check_lab1`.

// You will need to add private members to the class declaration in `stream_reassembler.hh`

template <typename... Targs>
void DUMMY_CODE(Targs &&... /* unused */) {}

using namespace std;

StreamReassembler::StreamReassembler(const size_t capacity) : 
_buffer(capacity, '\0'),_flag(capacity,false),
_output(capacity),_capacity(capacity) {}

//! \details This function accepts a substring (aka a segment) of bytes,
//! possibly out-of-order, from the logical stream, and assembles any newly
//! contiguous substrings and writes them into the output stream in order.
void StreamReassembler::push_substring(const string &data, const uint64_t index, const bool eof) {
    size_t _first_unassembled=_output.bytes_written();
    size_t _first_unaccept=_first_unassembled+_capacity;
    if(index>=_first_unaccept || index + data.length()<_first_unassembled){
        return;
    }

    size_t begin_index= max(index,_first_unassembled);
    size_t end_index= min(_first_unaccept,index+data.length());


    for(size_t i= begin_index; i<end_index;i++){
        if(!_flag[i-_first_unassembled]){
            _buffer[i-_first_unassembled]=data[i-index];
            _flag[i-_first_unassembled]=true;
            _unassembled_bytes++;
        }
    }
    string wait_str="";
    while(_flag.front()){
        wait_str+=_buffer.front();
        _buffer.pop_front();
        _flag.pop_front();
        _buffer.emplace_back('\0');
        _flag.emplace_back(false);
    }

    if(wait_str.length()>0){
        _output.write(wait_str);
        _unassembled_bytes-=wait_str.length();
    }


    if(eof){
        _is_eof=true;
        _eof_index=end_index;
    }

    if(_is_eof && _eof_index==_output.bytes_written()){
        _output.end_input();
    } 
}

size_t StreamReassembler::unassembled_bytes() const { return _unassembled_bytes; }

bool StreamReassembler::empty() const { return unassembled_bytes()==0; }
