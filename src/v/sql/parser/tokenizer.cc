// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/parser/tokenizer.h"

#include <cctype>
#include <string>

namespace redpanda::sql {

std::vector<token> tokenizer::tokenize() {
    std::vector<token> tokens;

    while (!is_at_end()) {
        skip_whitespace();
        if (is_at_end()) {
            break;
        }

        _start = _current;
        auto tok = scan_token();
        if (tok.token_type != token::type::INVALID) {
            tokens.push_back(std::move(tok));
        }
    }

    tokens.emplace_back(
      token::type::END_OF_FILE, "", _line, _column);
    return tokens;
}

char tokenizer::advance() {
    _column++;
    return _source[_current++];
}

char tokenizer::peek() const {
    if (is_at_end()) {
        return '\0';
    }
    return _source[_current];
}

char tokenizer::peek_next() const {
    if (_current + 1 >= _source.length()) {
        return '\0';
    }
    return _source[_current + 1];
}

bool tokenizer::match(char expected) {
    if (is_at_end() || _source[_current] != expected) {
        return false;
    }
    advance();
    return true;
}

void tokenizer::skip_whitespace() {
    while (!is_at_end()) {
        char c = peek();
        if (c == ' ' || c == '\t' || c == '\r') {
            advance();
        } else if (c == '\n') {
            _line++;
            _column = 0;
            advance();
        } else if (c == '-' && peek_next() == '-') {
            skip_line_comment();
        } else if (c == '/' && peek_next() == '*') {
            skip_block_comment();
        } else {
            break;
        }
    }
}

void tokenizer::skip_line_comment() {
    while (!is_at_end() && peek() != '\n') {
        advance();
    }
}

void tokenizer::skip_block_comment() {
    advance(); // /
    advance(); // *
    while (!is_at_end()) {
        if (peek() == '*' && peek_next() == '/') {
            advance();
            advance();
            break;
        }
        if (peek() == '\n') {
            _line++;
            _column = 0;
        }
        advance();
    }
}

token tokenizer::scan_token() {
    char c = advance();

    switch (c) {
    case '(':
        return make_token(token::type::LEFT_PAREN);
    case ')':
        return make_token(token::type::RIGHT_PAREN);
    case '[':
        return make_token(token::type::LEFT_BRACKET);
    case ']':
        return make_token(token::type::RIGHT_BRACKET);
    case '{':
        return make_token(token::type::LEFT_BRACE);
    case '}':
        return make_token(token::type::RIGHT_BRACE);
    case ',':
        return make_token(token::type::COMMA);
    case ';':
        return make_token(token::type::SEMICOLON);
    case '.':
        return make_token(token::type::DOT);
    case ':':
        return make_token(token::type::COLON);
    case '+':
        return make_token(token::type::PLUS);
    case '-':
        if (match('>')) {
            return make_token(token::type::ARROW);
        }
        return make_token(token::type::MINUS);
    case '*':
        return make_token(token::type::STAR);
    case '/':
        return make_token(token::type::SLASH);
    case '%':
        return make_token(token::type::PERCENT);
    case '=':
        return make_token(token::type::EQUAL);
    case '!':
        if (match('=')) {
            return make_token(token::type::NOT_EQUAL);
        }
        return make_token(token::type::INVALID);
    case '<':
        if (match('=')) {
            return make_token(token::type::LESS_EQUAL);
        }
        if (match('>')) {
            return make_token(token::type::NOT_EQUAL);
        }
        return make_token(token::type::LESS);
    case '>':
        if (match('=')) {
            return make_token(token::type::GREATER_EQUAL);
        }
        return make_token(token::type::GREATER);
    case '|':
        if (match('|')) {
            return make_token(token::type::CONCAT);
        }
        return make_token(token::type::INVALID);
    case '\'':
    case '"':
        return string_literal();
    default:
        if (is_digit(c)) {
            return number();
        }
        if (is_alpha(c)) {
            return identifier_or_keyword();
        }
        return make_token(token::type::INVALID);
    }
}

token tokenizer::make_token(token::type type) {
    size_t length = _current - _start;
    auto lexeme = ss::sstring(
      _source.substr(_start, length).data(), length);
    return token{type, std::move(lexeme), _line, _column - length};
}

token tokenizer::identifier_or_keyword() {
    while (is_alphanumeric(peek())) {
        advance();
    }
    return make_token(identifier_type());
}

token tokenizer::number() {
    while (is_digit(peek())) {
        advance();
    }

    // Look for decimal point
    if (peek() == '.' && is_digit(peek_next())) {
        advance(); // consume '.'
        while (is_digit(peek())) {
            advance();
        }
        return make_token(token::type::FLOAT_LITERAL);
    }

    return make_token(token::type::INTEGER_LITERAL);
}

token tokenizer::string_literal() {
    char quote = _source[_current - 1];

    while (!is_at_end() && peek() != quote) {
        if (peek() == '\n') {
            _line++;
            _column = 0;
        }
        advance();
    }

    if (is_at_end()) {
        return make_token(token::type::INVALID);
    }

    advance(); // closing quote
    return make_token(token::type::STRING_LITERAL);
}

token::type tokenizer::identifier_type() {
    // Simple keyword recognition
    std::string_view lexeme = _source.substr(_start, _current - _start);

    // Convert to uppercase for comparison
    std::string upper;
    upper.reserve(lexeme.size());
    for (char c : lexeme) {
        upper += std::toupper(c);
    }

    // Check keywords
    if (upper == "SELECT")
        return token::type::SELECT;
    if (upper == "FROM")
        return token::type::FROM;
    if (upper == "WHERE")
        return token::type::WHERE;
    if (upper == "GROUP")
        return token::type::GROUP;
    if (upper == "BY")
        return token::type::BY;
    if (upper == "HAVING")
        return token::type::HAVING;
    if (upper == "ORDER")
        return token::type::ORDER;
    if (upper == "JOIN")
        return token::type::JOIN;
    if (upper == "INNER")
        return token::type::INNER;
    if (upper == "LEFT")
        return token::type::LEFT;
    if (upper == "RIGHT")
        return token::type::RIGHT;
    if (upper == "FULL")
        return token::type::FULL;
    if (upper == "OUTER")
        return token::type::OUTER;
    if (upper == "ON")
        return token::type::ON;
    if (upper == "AS")
        return token::type::AS;
    if (upper == "CREATE")
        return token::type::CREATE;
    if (upper == "TABLE")
        return token::type::TABLE;
    if (upper == "STREAM")
        return token::type::STREAM;
    if (upper == "MATERIALIZED")
        return token::type::MATERIALIZED;
    if (upper == "VIEW")
        return token::type::VIEW;
    if (upper == "WITH")
        return token::type::WITH;
    if (upper == "AND")
        return token::type::AND;
    if (upper == "OR")
        return token::type::OR;
    if (upper == "NOT")
        return token::type::NOT;
    if (upper == "IN")
        return token::type::IN;
    if (upper == "LIKE")
        return token::type::LIKE;
    if (upper == "BETWEEN")
        return token::type::BETWEEN;
    if (upper == "IS")
        return token::type::IS;
    if (upper == "NULL")
        return token::type::NULL_TOKEN;
    if (upper == "TRUE")
        return token::type::TRUE_TOKEN;
    if (upper == "FALSE")
        return token::type::FALSE_TOKEN;
    if (upper == "DISTINCT")
        return token::type::DISTINCT;
    if (upper == "COUNT")
        return token::type::COUNT;
    if (upper == "SUM")
        return token::type::SUM;
    if (upper == "AVG")
        return token::type::AVG;
    if (upper == "MIN")
        return token::type::MIN;
    if (upper == "MAX")
        return token::type::MAX;
    if (upper == "WINDOW")
        return token::type::WINDOW;
    if (upper == "OVER")
        return token::type::OVER;
    if (upper == "PARTITION")
        return token::type::PARTITION;
    if (upper == "INTERVAL")
        return token::type::INTERVAL;
    if (upper == "TUMBLE")
        return token::type::TUMBLE;
    if (upper == "HOP")
        return token::type::HOP;
    if (upper == "SESSION")
        return token::type::SESSION;

    return token::type::IDENTIFIER;
}

} // namespace redpanda::sql
