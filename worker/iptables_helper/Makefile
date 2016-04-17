BUILD=./build

OBJS=iptables_helper.o
CFLAGS=-Wall
LDFLAGS=

all: $(BUILD) $(BUILD)/iptables_helper set_root

.PHONY: clean set_root
clean:
	rm -r $(BUILD)

set_root: $(BUILD)/iptables_helper
	sudo chown root $(BUILD)/iptables_helper
	sudo chmod u+s $(BUILD)/iptables_helper

$(BUILD)/iptables_helper: $(addprefix $(BUILD)/,$(OBJS))
	$(CC) $(LDFLAGS) -o $@ $^

$(BUILD)/%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $< 

$(BUILD):
	mkdir -p $(BUILD)


